const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const ROOT_DIR = path.resolve(__dirname, '..');
const DOCS_DIR = path.join(ROOT_DIR, 'docs');
const OUT_FILE = path.join(DOCS_DIR, 'changelog.md');

function run(command) {
  return execSync(command, { cwd: ROOT_DIR, encoding: 'utf8' }).trim();
}

function getTags() {
  return run('git tag --sort=-v:refname')
    .split('\n')
    .filter(Boolean);
}

function getNextVersion() {
  const arg = process.argv[2];
  return arg ? (arg.startsWith('v') ? arg : `v${arg}`) : null;
}

function today() {
  return new Date().toISOString().split('T')[0];
}

function getHeadCommits(sinceTag) {
  const range = sinceTag ? `${sinceTag}..HEAD` : 'HEAD';
  try {
    return run(`git log --pretty=format:'%s' --no-merges ${range}`)
      .split('\n')
      .filter(Boolean);
  } catch {
    return [];
  }
}

function renderReleaseSection(tag, date, commits) {
  const githubRelease = `https://github.com/emanuel-epifani/nexo/releases/tag/${tag}`;
  const dockerTag = `https://hub.docker.com/r/emanuelepifani/nexo/tags?name=${tag.replace(/^v/, '')}`;
  const npmTag = `https://www.npmjs.com/package/@emanuelepifani/nexo-client/v/${tag.replace(/^v/, '')}`;
  const pypiTag = `https://pypi.org/project/nexo-client/${tag.replace(/^v/, '')}/`;

  let section = `## ${tag}\n\n`;
  section += `**Released:** ${date || 'unknown'}\n\n`;
  section += `<p class="release-downloads">\n`;
  section += `  <a class="download-pill" href="${githubRelease}" target="_blank" rel="noreferrer">Download ${tag}</a>\n`;
  section += `  <a class="download-pill alt" href="${dockerTag}" target="_blank" rel="noreferrer">Docker tag ${tag}</a>\n`;
  section += `  <a class="download-pill npm" href="${npmTag}" target="_blank" rel="noreferrer">npm ${tag}</a>\n`;
  section += `  <a class="download-pill pypi" href="${pypiTag}" target="_blank" rel="noreferrer">PyPI ${tag}</a>\n`;
  section += `</p>\n\n`;

  if (commits.length === 0) {
    section += '_No recorded changes for this release._\n\n';
    return section;
  }

  const categories = categorize(commits);
  for (const [category, items] of categories) {
    section += `### ${category}\n\n`;
    for (const item of items) {
      section += `- ${item}\n`;
    }
    section += '\n';
  }

  return section;
}

function getTagDate(tag) {
  try {
    return run(`git log -1 --format=%ai ${tag}`).split(' ')[0];
  } catch {
    return '';
  }
}

function getCommits(tag, previousTag) {
  const range = previousTag ? `${previousTag}..${tag}` : tag;
  try {
    return run(`git log --pretty=format:'%s' --no-merges ${range}`)
      .split('\n')
      .filter(Boolean);
  } catch {
    return [];
  }
}

function categorize(commits) {
  const groups = {
    Added: [],
    Changed: [],
    Removed: [],
    Fixed: [],
    Performance: [],
  };

  const EXCLUDED_TYPES = ['chore', 'docs', 'test', 'ci', 'build'];

  for (const commit of commits) {
    const match = commit.match(/^(\w+)(?:\([^)]+\))?:\s*(.*)$/);
    const type = match ? match[1].toLowerCase() : '';
    const body = match ? match[2] : commit;

    // Skip non-user-facing types
    if (EXCLUDED_TYPES.includes(type)) continue;

    // Skip release/bump commits
    if (type === 'chore' || type === '') {
      const normalized = commit.toLowerCase();
      if (normalized.includes('release v') || normalized.includes('bump version')) continue;
    }

    let category = 'Changed';

    if (type === 'feat') category = 'Added';
    else if (type === 'fix') category = 'Fixed';
    else if (type === 'perf') category = 'Performance';
    else if (type === 'refactor') category = 'Changed';
    else if (type === 'revert') category = 'Removed';
    else {
      // Keyword fallback for untyped commits
      const normalized = commit.toLowerCase();
      if (normalized.includes('remove') || normalized.includes('rimosso') || normalized.includes('drop')) category = 'Removed';
    }

    let text = match ? `${body.charAt(0).toUpperCase()}${body.slice(1)}` : commit;
    text = text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
    groups[category].push(text);
  }

  return Object.entries(groups).filter(([, items]) => items.length > 0);
}

function build() {
  const tags = getTags();
  if (tags.length === 0) {
    console.error('No git tags found.');
    process.exit(1);
  }

  const nextVersion = getNextVersion();

  let out = `---
title: Changelog
description: Release notes and download links for every Nexo version.
outline:
  level: [2, 2]
---

# Changelog

Release notes for the Nexo broker and SDKs.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

`;

  // When a future version is passed (e.g., from the release script), render it
  // first using commits between the latest tag and HEAD.
  if (nextVersion) {
    out += renderReleaseSection(nextVersion, today(), getHeadCommits(tags[0]));
  }

  for (let i = 0; i < tags.length; i++) {
    const tag = tags[i];
    const previousTag = tags[i + 1];
    const date = getTagDate(tag);
    const commits = getCommits(tag, previousTag);
    out += renderReleaseSection(tag, date, commits);
  }

  fs.writeFileSync(OUT_FILE, out);
  console.log(`✅ Wrote ${OUT_FILE} (${tags.length} versions)`);
}

build();
