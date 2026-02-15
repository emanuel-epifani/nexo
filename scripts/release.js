const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { execSync } = require('child_process');

const ROOT_DIR = path.resolve(__dirname, '..');

const FILES = [
    { 
        path: 'Cargo.toml', 
        type: 'toml',
        regex: /^version = "(.*)"/m 
    },
    { 
        path: 'sdk/ts/package.json', 
        type: 'json' 
    },
    { 
        path: 'dashboard/package.json', 
        type: 'json' 
    },
    { 
        path: 'nexo-docs-hub/package.json', 
        type: 'json' 
    }
];

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

function run(command) {
    try {
        return execSync(command, { cwd: ROOT_DIR, encoding: 'utf8' }).trim();
    } catch (e) {
        console.error(`Command failed: ${command}`);
        process.exit(1);
    }
}

function getCurrentVersion() {
    const cargoPath = path.join(ROOT_DIR, 'Cargo.toml');
    const content = fs.readFileSync(cargoPath, 'utf8');
    const match = content.match(/^version = "(.*)"/m);
    return match ? match[1] : 'unknown';
}

function updateVersion(newVersion) {
    console.log(`\nUpdating files to v${newVersion}...`);
    
    for (const file of FILES) {
        const filePath = path.join(ROOT_DIR, file.path);
        let content = fs.readFileSync(filePath, 'utf8');
        
        if (file.type === 'json') {
            const json = JSON.parse(content);
            json.version = newVersion;
            content = JSON.stringify(json, null, 2) + '\n';
        } else if (file.type === 'toml') {
            content = content.replace(file.regex, `version = "${newVersion}"`);
        }
        
        fs.writeFileSync(filePath, content);
        console.log(`✅ Updated ${file.path}`);
    }
}

function getRecentCommits() {
    try {
        const lastTag = run('git describe --tags --abbrev=0 2>/dev/null || echo ""');
        const range = lastTag ? `${lastTag}..HEAD` : 'HEAD';
        const logs = run(`git log ${range} --pretty=format:"- %s"`);
        return logs;
    } catch (e) {
        return "No commits found or error retrieving logs.";
    }
}

console.log("🚀 NEXO RELEASE WIZARD");
console.log("======================");

const currentVer = getCurrentVersion();
console.log(`Current Version: ${currentVer}`);

rl.question('Enter new version (e.g., 0.3.1): ', (newVersion) => {
    if (!newVersion) {
        console.log("Operation cancelled.");
        rl.close();
        return;
    }

    // 1. Validations
    if (!/^\d+\.\d+\.\d+$/.test(newVersion)) {
        console.error("❌ Invalid version format. Use X.Y.Z");
        rl.close();
        return;
    }

    // 2. Show Changelog Candidates
    console.log("\n📝 Commits since last tag:");
    console.log(getRecentCommits());
    console.log("\n(Copy these to CHANGELOG.md if needed)");

    rl.question('\nProceed with update? (y/N): ', (answer) => {
        if (answer.toLowerCase() !== 'y') {
            console.log("Aborted.");
            rl.close();
            return;
        }

        // 3. Update Files
        updateVersion(newVersion);

        // 4. Git Operations
        console.log("\n📦 Staging files...");
        run('git add Cargo.toml sdk/ts/package.json dashboard/package.json nexo-docs-hub/package.json');
        
        // Check if CHANGELOG.md is modified
        const status = run('git status --porcelain');
        if (status.includes('CHANGELOG.md')) {
            run('git add CHANGELOG.md');
            console.log("✅ Included CHANGELOG.md");
        }

        console.log("💾 Committing...");
        run(`git commit -m "chore: release v${newVersion}"`);

        console.log(`🏷️  Tagging v${newVersion}...`);
        run(`git tag v${newVersion}`);

        console.log("\n🎉 Release ready!");
        console.log(`Run 'git push && git push --tags' to publish.`);
        
        rl.close();
    });
});
