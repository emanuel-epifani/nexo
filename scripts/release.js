const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { execSync } = require("child_process");

const ROOT_DIR = path.resolve(__dirname, "..");

const FILES = [
  {
    path: "Cargo.toml",
    type: "toml",
    regex: /^version = "(.*?)"/m,
  },
  {
    path: "sdk/ts/package.json",
    type: "json",
  },
  {
    path: "sdk/py/pyproject.toml",
    type: "toml",
    regex: /^version = "(.*?)"/m,
  },
  {
    path: "docs/package.json",
    type: "json",
  },
];

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

function ask(question) {
  return new Promise((resolve) => rl.question(question, resolve));
}

function run(command) {
  try {
    return execSync(command, { cwd: ROOT_DIR, encoding: "utf8" }).trim();
  } catch (e) {
    console.error(`Command failed: ${command}`);
    process.exit(1);
  }
}

function getCurrentVersion() {
  const cargoPath = path.join(ROOT_DIR, "Cargo.toml");
  const content = fs.readFileSync(cargoPath, "utf8");
  const match = content.match(/^version = "(.*?)"/m);
  return match ? match[1] : "unknown";
}

function bumpVersion(current, type) {
  const [major, minor, patch] = current.split(".").map(Number);
  switch (type) {
    case "major":
      return `${major + 1}.0.0`;
    case "minor":
      return `${major}.${minor + 1}.0`;
    case "patch":
      return `${major}.${minor}.${patch + 1}`;
    default:
      throw new Error(`Unknown bump type: ${type}`);
  }
}

function updateVersion(newVersion) {
  console.log(`\nUpdating files to v${newVersion}...`);

  for (const file of FILES) {
    const filePath = path.join(ROOT_DIR, file.path);

    if (!fs.existsSync(filePath)) {
      console.log(`⚠️  Skipped ${file.path} (file not found)`);
      continue;
    }

    let content = fs.readFileSync(filePath, "utf8");

    if (file.type === "json") {
      const json = JSON.parse(content);
      json.version = newVersion;
      content = JSON.stringify(json, null, 2) + "\n";
    } else if (file.type === "toml") {
      content = content.replace(file.regex, `version = "${newVersion}"`);
    }

    fs.writeFileSync(filePath, content);
    console.log(`✅ Updated ${file.path}`);
  }
}

function getRecentCommits() {
  try {
    const lastTag = run(
      'git describe --tags --abbrev=0 2>/dev/null || echo ""',
    );
    const range = lastTag ? `${lastTag}..HEAD` : "HEAD";
    const logs = run(`git log ${range} --pretty=format:"- %s"`);
    return logs || "No new commits since last tag.";
  } catch (e) {
    return "No commits found or error retrieving logs.";
  }
}

function checkCleanWorkspace() {
  const status = run("git status --porcelain");
  if (status) {
    console.error(
      "❌ Git workspace is dirty. Please commit or stash your changes before releasing.",
    );
    console.error("Uncommitted changes:");
    console.error(status);
    process.exit(1);
  }
}

async function main() {
  console.log("🚀 NEXO RELEASE WIZARD");
  console.log("======================\n");

  checkCleanWorkspace();

  const currentVer = getCurrentVersion();
  console.log(`Current Version: ${currentVer}\n`);

  // Show recent commits for context
  console.log("📝 Commits since last tag:");
  console.log(getRecentCommits());
  console.log("");

  // Auto-bump selection
  const patchVer = bumpVersion(currentVer, "patch");
  const minorVer = bumpVersion(currentVer, "minor");
  const majorVer = bumpVersion(currentVer, "major");

  console.log("What kind of release?");
  console.log(`  [1] patch  → ${patchVer}  (bugfix, internal improvements)`);
  console.log(
    `  [2] minor  → ${minorVer}  (new features, backward compatible)`,
  );
  console.log(`  [3] major  → ${majorVer}  (breaking changes)`);
  console.log(`  [4] custom → enter manually\n`);

  const choice = await ask("Choice (1/2/3/4): ");

  let newVersion;
  switch (choice.trim()) {
    case "1":
      newVersion = patchVer;
      break;
    case "2":
      newVersion = minorVer;
      break;
    case "3":
      newVersion = majorVer;
      break;
    case "4":
      newVersion = await ask("Enter version (e.g., 0.3.1): ");
      if (!newVersion || !/^\d+\.\d+\.\d+$/.test(newVersion.trim())) {
        console.error("❌ Invalid version format. Use X.Y.Z");
        rl.close();
        return;
      }
      newVersion = newVersion.trim();
      break;
    default:
      console.log("Aborted.");
      rl.close();
      return;
  }

  // Confirmation
  console.log(`\n📋 Release Summary:`);
  console.log(`   ${currentVer} → ${newVersion}`);
  console.log(`   Files to update:`);
  for (const file of FILES) {
    console.log(`     • ${file.path}`);
  }

  const confirm = await ask("\nProceed? (y/N): ");
  if (confirm.toLowerCase() !== "y") {
    console.log("Aborted.");
    rl.close();
    return;
  }

  // 1. Static checks (fail fast before bumping version)
  console.log("\n🔍 Running static checks...");

  console.log("  • TypeScript type check");
  run("cd sdk/ts && npx tsc --noEmit");

  console.log("  • Python type check");
  run("cd sdk/py && mypy src/nexo");

  console.log("  • Protocol constants in sync with protocol.json");
  run("node scripts/generate-protocol.js --check");

  console.log("  • Docs build");
  run("cd docs && npm run build");

  console.log("✅ All static checks passed\n");

  // 2. Update version in all files
  updateVersion(newVersion);

  // 2c. Update public changelog (renders the new version at the top)
  console.log("📝 Updating docs changelog...");
  run(`node scripts/build-changelog.js ${newVersion}`);

  // 2b. Update lockfiles
  console.log("📦 Updating Cargo.lock...");
  run("cargo check"); // This updates Cargo.lock automatically

  console.log("📦 Updating sdk/ts/package-lock.json...");
  run("cd sdk/ts && npm install --package-lock-only");

  console.log("📦 Updating sdk/py/uv.lock...");
  run("cd sdk/py && uv lock");

  // 3. Stage files
  console.log("\n📦 Staging files...");
  const filesToStage = [
    ...FILES.map((f) => f.path),
    "docs/changelog.md",
    "Cargo.lock",
    "sdk/ts/package-lock.json",
    "sdk/py/uv.lock",
    "protocol.json",
    "scripts/generate-protocol.js",
    "src/protocol/generated.rs",
    "sdk/ts/src/protocol.ts",
    "sdk/py/src/nexo/protocol.py",
  ].filter((p) => fs.existsSync(path.join(ROOT_DIR, p)));
  run(`git add ${filesToStage.join(" ")}`);

  // 4. Commit
  console.log("💾 Committing...");
  run(`git commit -m "chore: release v${newVersion}"`);

  // 5. Tag
  console.log(`🏷️  Tagging v${newVersion}...`);
  run(`git tag -a v${newVersion} -m "Release v${newVersion}"`);

  // 5. Push
  const pushNow = await ask(
    "\n🚀 Push now? (this will trigger the deploy pipeline) (y/N): ",
  );
  if (pushNow.toLowerCase() === "y") {
    console.log("Pushing to remote...");
    run("git push");
    run("git push --tags");
    console.log("\n🎉 Release v" + newVersion + " pushed!");
    console.log(
      "   The GitHub Actions pipeline will now deploy automatically:",
    );
    console.log("   • 🐳 Docker image → Docker Hub");
    console.log("   • 📦 SDK → NPM");
    console.log("   • 📄 Docs → Vercel/Netlify");
    console.log(
      "\n   Monitor progress: https://github.com/emanuel-epifani/nexo/actions",
    );
  } else {
    console.log("\n🎉 Release v" + newVersion + " ready!");
    console.log("   Run this when you're ready to deploy:");
    console.log("   git push && git push --tags");
  }

  rl.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  rl.close();
  process.exit(1);
});
