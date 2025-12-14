# Use the official Playwright image which comes with browsers and Node.js
FROM mcr.microsoft.com/playwright:v1.44.0-jammy

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json first to leverage Docker layer caching
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of your application's code into the container
COPY . .

# Expose the port the app runs on
EXPOSE 3000

# The command to run your application
CMD ["node", "server.js"]
