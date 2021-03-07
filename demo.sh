#!/usr/bin/env bash
set -euo pipefail

# docker build . -t reviews
docker run -it --network="tp3_reviews_network" -v "$(cd .. && pwd)/data:/data" reviews 

