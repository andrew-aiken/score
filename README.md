![License](https://img.shields.io/badge/License-GLP%203.0-blue.svg)
![GitHub Release](https://img.shields.io/github/v/release/andrew-aiken/score)
![Tests](https://img.shields.io/github/actions/workflow/status/andrew-aiken/score/test.yaml)

### Features:

- Distributed agent design
- Minimalist design
  - Built for scoring, not injects
  - Wide variety of configurable [checks](https://github.com/andrew-aiken/checks)
- [Documentation](https://github.com/andrew-aiken/score/wiki) & tests

## Quick Start

```bash
# Pull the code locally
git clone https://github.com/andrew-aiken/score.git

# Initialize new certificates for user signing and nats server authentication
docker run --rm -it -v $(pwd)/scripts/:/scripts:ro -v $(pwd)/nsc:/nsc --entrypoint '/scripts/setup.sh' natsio/nats-box:latest

# Generate localhost TLS certificates
bash ./caddy/certs/generate.sh

# Launch the docker compose stack
docker compose up --build

# Open https://localhost
```

## Similar Projects

- [Quotient](https://github.com/dbaseqp/Quotient)
- [Scorestack](https://github.com/scorestack/scorestack)
- [Scorify](https://github.com/Scorify/Scorify)
- [Scoring Engine](https://github.com/scoringengine/scoringengine) <!-- checks are just cli commands & bash scripts -->
- [Scoring-Engine (C2 Games)](https://gitlab.com/c2-games/scoring) <!-- Not going to lie, this one is all over the place -->
