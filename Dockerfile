# 1. Start from an official, lightweight Python image
FROM python:3.10-slim

# 2. Set the working directory inside the container to '/app'
# All our commands will run from here
WORKDIR /app

# 3. Copy *only* the requirements file first
# This is a Docker trick: it caches this layer so it doesn't 
# re-install all your libraries every time you change your code.
COPY requirements.txt .

# 4. Install all the Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy your *entire* project (src, sql, etc.) into the /app directory
COPY . .

# (We don't need a CMD or ENTRYPOINT, because our
# docker-compose.yml file provides the command to run)