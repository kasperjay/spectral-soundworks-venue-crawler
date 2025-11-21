# Use Playwright-compatible base image with Chromium
FROM apify/actor-node-playwright-chrome:22

# Copy package definition(s) first for better caching
COPY package*.json ./

# Install production dependencies only (skip dev & optional)
RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && echo "Installed NPM packages:" \
    && npm list --omit=dev --all || true \
    && echo "Node.js version:" \
    && node --version \
    && echo "NPM version:" \
    && npm --version

# Copy your source code into the container
COPY . ./

# Run your actor entry point

CMD ["npm", "start"]