# Stage 1: BUILD
# Use Node to build the React app
FROM node:18-alpine AS build

WORKDIR /app

# Copy package files first (caching trick)
COPY package*.json ./
RUN npm install

# Copy all source code
COPY . .

# Build production version of React app
# Creates /app/build folder with optimized files
RUN npm run build

# Stage 2: SERVE
# Use Nginx to serve the built files
# Much smaller than keeping Node running
FROM nginx:alpine

# Copy built React files into Nginx
COPY --from=build /app/build /usr/share/nginx/html

# Copy our Nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Expose port 80
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]