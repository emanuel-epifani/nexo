const fs = require("fs");
const path = require("path");

const ROOT_DIR = path.resolve(__dirname, "..");
const SPEC_PATH = path.join(ROOT_DIR, "protocol.json");
const CHECK_ONLY = process.argv.includes("--check");

const spec = JSON.parse(fs.readFileSync(SPEC_PATH, "utf8"));
const enumNames = {
  store: "StoreOpcode",
  queue: "QueueOpcode",
  pubsub: "PubSubOpcode",
  stream: "StreamOpcode",
};

function assertInteger(name, value, min, max) {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
}

function validateByteMap(name, values, requireUnique = true) {
  const seen = new Set();
  for (const [key, value] of Object.entries(values)) {
    assertInteger(`${name}.${key}`, value, 0, 255);
    if (requireUnique && seen.has(value)) {
      throw new Error(`${name} contains duplicate value ${value}`);
    }
    seen.add(value);
  }
}

function validateSpec() {
  assertInteger("protocolVersion", spec.protocolVersion, 0, 255);
  assertInteger("header.size", spec.header.size, 1, 255);
  validateByteMap("header.offsets", spec.header.offsets);
  for (const [key, value] of Object.entries(spec.header.offsets)) {
    if (value >= spec.header.size) {
      throw new Error(`header.offsets.${key} must be smaller than header.size`);
    }
  }
  validateByteMap("frameTypes", spec.frameTypes);
  validateByteMap("responseStatuses", spec.responseStatuses);
  validateByteMap("dataTypes", spec.dataTypes);

  const usedOpcodes = new Map();
  for (const [broker, definition] of Object.entries(spec.opcodes)) {
    validateByteMap(`opcodes.${broker}.values`, definition.values);
    if (definition.range) {
      const [min, max] = definition.range;
      assertInteger(`opcodes.${broker}.range[0]`, min, 0, 255);
      assertInteger(`opcodes.${broker}.range[1]`, max, min, 255);
      for (const [name, value] of Object.entries(definition.values)) {
        if (value < min || value > max) {
          throw new Error(
            `opcodes.${broker}.${name} is outside its broker range`,
          );
        }
      }
    }
    for (const [name, value] of Object.entries(definition.values)) {
      const previous = usedOpcodes.get(value);
      if (previous) {
        throw new Error(
          `opcode ${value} is assigned to both ${previous} and ${broker}.${name}`,
        );
      }
      usedOpcodes.set(value, `${broker}.${name}`);
    }
  }

  for (const [name, value] of Object.entries(spec.limits)) {
    assertInteger(`limits.${name}`, value, 1, Number.MAX_SAFE_INTEGER);
  }

  for (const [broker, commands] of Object.entries(spec.commandFlags || {})) {
    for (const [command, flags] of Object.entries(commands)) {
      validateByteMap(`commandFlags.${broker}.${command}`, flags);
    }
  }
}

function hex(value) {
  return `0x${value.toString(16).toUpperCase().padStart(2, "0")}`;
}

function number(value) {
  return value.toLocaleString("en-US").replaceAll(",", "_");
}

function renderRust() {
  const lines = [
    `pub const PROTOCOL_VERSION: u8 = ${hex(spec.protocolVersion)};`,
    `pub const HEADER_SIZE: usize = ${spec.header.size};`,
  ];
  for (const [name, value] of Object.entries(spec.header.offsets)) {
    lines.push(`pub const HEADER_OFFSET_${name}: usize = ${value};`);
  }
  lines.push("");
  for (const [name, value] of Object.entries(spec.frameTypes)) {
    lines.push(`pub const TYPE_${name}: u8 = ${hex(value)};`);
  }
  lines.push("");
  for (const [name, value] of Object.entries(spec.responseStatuses)) {
    lines.push(`pub const STATUS_${name}: u8 = ${hex(value)};`);
  }
  lines.push("");
  for (const [name, value] of Object.entries(spec.dataTypes)) {
    lines.push(`pub const DATA_TYPE_${name}: u8 = ${hex(value)};`);
  }
  lines.push("");

  for (const [broker, definition] of Object.entries(spec.opcodes)) {
    const brokerName = broker.toUpperCase();
    if (definition.range) {
      lines.push(
        `pub const ${brokerName}_OPCODE_MIN: u8 = ${hex(definition.range[0])};`,
      );
      lines.push(
        `pub const ${brokerName}_OPCODE_MAX: u8 = ${hex(definition.range[1])};`,
      );
    }
    for (const [name, value] of Object.entries(definition.values)) {
      const constant = broker === "debug" ? `OP_DEBUG_${name}` : `OP_${name}`;
      lines.push(`pub const ${constant}: u8 = ${hex(value)};`);
    }
    lines.push("");
  }

  for (const [name, value] of Object.entries(spec.limits)) {
    lines.push(`pub const ${name}: usize = ${number(value)};`);
  }

  if (spec.commandFlags) {
    lines.push("");
    for (const [broker, commands] of Object.entries(spec.commandFlags)) {
      for (const [command, flags] of Object.entries(commands)) {
        for (const [flagName, flagValue] of Object.entries(flags)) {
          lines.push(
            `pub const FLAG_${broker.toUpperCase()}_${command}_${flagName}: u8 = ${hex(flagValue)};`,
          );
        }
      }
    }
  }
  return `${lines.join("\n")}\n`;
}

function renderTypeScript() {
  const lines = [
    `export const PROTOCOL_VERSION = ${hex(spec.protocolVersion)};`,
    "",
    "export enum FrameType {",
  ];
  for (const [name, value] of Object.entries(spec.frameTypes)) {
    lines.push(`  ${name} = ${hex(value)},`);
  }
  lines.push("}", "", "export enum ResponseStatus {");
  for (const [name, value] of Object.entries(spec.responseStatuses)) {
    lines.push(`  ${name} = ${hex(value)},`);
  }
  lines.push("}", "", "export enum DataType {");
  for (const [name, value] of Object.entries(spec.dataTypes)) {
    lines.push(`  ${name} = ${hex(value)},`);
  }
  lines.push(
    "}",
    "",
    `export const HEADER_SIZE = ${spec.header.size};`,
    "export const HEADER_OFFSET = {",
  );
  for (const [name, value] of Object.entries(spec.header.offsets)) {
    lines.push(`  ${name}: ${value},`);
  }
  lines.push("} as const;", "");

  for (const [broker, enumName] of Object.entries(enumNames)) {
    lines.push(`export enum ${enumName} {`);
    for (const [name, value] of Object.entries(spec.opcodes[broker].values)) {
      lines.push(`  ${name} = ${hex(value)},`);
    }
    lines.push("}", "");
  }

  for (const [name, value] of Object.entries(spec.limits)) {
    lines.push(`export const ${name} = ${number(value)};`);
  }

  if (spec.commandFlags) {
    lines.push("");
    for (const [broker, commands] of Object.entries(spec.commandFlags)) {
      for (const [command, flags] of Object.entries(commands)) {
        for (const [flagName, flagValue] of Object.entries(flags)) {
          lines.push(
            `export const FLAG_${broker.toUpperCase()}_${command}_${flagName} = ${hex(flagValue)};`,
          );
        }
      }
    }
  }
  return `${lines.join("\n")}\n`;
}

function renderPython() {
  const lines = [
    "from enum import IntEnum",
    "",
    `PROTOCOL_VERSION = ${hex(spec.protocolVersion)}`,
    "",
    `HEADER_SIZE = ${spec.header.size}`,
  ];
  for (const [name, value] of Object.entries(spec.header.offsets)) {
    lines.push(`HEADER_OFFSET_${name} = ${value}`);
  }
  lines.push("", "", "class FrameType(IntEnum):");
  for (const [name, value] of Object.entries(spec.frameTypes)) {
    lines.push(`    ${name} = ${hex(value)}`);
  }
  lines.push("", "", "class ResponseStatus(IntEnum):");
  for (const [name, value] of Object.entries(spec.responseStatuses)) {
    lines.push(`    ${name} = ${hex(value)}`);
  }
  lines.push("", "", "class DataType(IntEnum):");
  for (const [name, value] of Object.entries(spec.dataTypes)) {
    lines.push(`    ${name} = ${hex(value)}`);
  }
  lines.push("");

  for (const [broker, className] of Object.entries(enumNames)) {
    lines.push("", `class ${className}:`);
    for (const [name, value] of Object.entries(spec.opcodes[broker].values)) {
      lines.push(`    ${name} = ${hex(value)}`);
    }
  }
  lines.push("");

  for (const [name, value] of Object.entries(spec.limits)) {
    lines.push(`${name} = ${number(value)}`);
  }

  if (spec.commandFlags) {
    lines.push("");
    for (const [broker, commands] of Object.entries(spec.commandFlags)) {
      for (const [command, flags] of Object.entries(commands)) {
        for (const [flagName, flagValue] of Object.entries(flags)) {
          lines.push(
            `FLAG_${broker.toUpperCase()}_${command}_${flagName} = ${hex(flagValue)}`,
          );
        }
      }
    }
  }
  return `${lines.join("\n")}\n`;
}

function updateFile(relativePath, expected) {
  const filePath = path.join(ROOT_DIR, relativePath);
  if (CHECK_ONLY) {
    const current = fs.existsSync(filePath)
      ? fs.readFileSync(filePath, "utf8")
      : null;
    if (current !== expected) {
      throw new Error(`${relativePath} is not generated from protocol.json`);
    }
    return;
  }
  fs.writeFileSync(filePath, expected);
}

validateSpec();
updateFile("src/protocol/generated.rs", renderRust());
updateFile("sdk/ts/src/protocol/generated.ts", renderTypeScript());
updateFile("sdk/py/src/nexo/protocol/generated.py", renderPython());
