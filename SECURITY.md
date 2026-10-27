# Security Policy

## Reporting a vulnerability

Please report security issues **privately**. Do not open a public issue for a
suspected vulnerability.

- Preferred: open a private [GitHub Security Advisory](https://github.com/lucheeseng827/freshet/security/advisories/new)
  with details and reproduction steps.
- If you cannot use Security Advisories, contact the maintainer through their GitHub
  profile ([@lucheeseng827](https://github.com/lucheeseng827)) to arrange a private channel.

We aim to acknowledge reports within a few business days and follow a 90-day
coordinated disclosure window, crediting reporters who wish to be named.

## Supported versions

Until 1.0, the latest published `0.x` minor is supported.

| Version | Supported |
|---------|-----------|
| latest `0.x` | ✅ |
| older | ❌ |

## Security model — read before you deploy

Freshet parses SQL and never executes it: no surface opens a warehouse connection.

- **`freshet-serve` has no authentication in this version.** It binds to
  `127.0.0.1` by default and prints a warning if you bind anything else. Keep it on
  loopback or a trusted network; anyone who can reach it can read the project it
  serves and commit its state.
- **Hosted state tokens come from the environment only** (`FRESHET_STATE_TOKEN`),
  never from a flag, so they stay out of shell history and process listings.
- **State files are trusted input.** `state.json` decides what is considered already
  built; whoever can write it can make a plan skip work. Protect it like the code.
