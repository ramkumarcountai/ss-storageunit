FROM countaiadmin/base_storageunit:1.0.0
# Set working directory
WORKDIR /app
# Copy the main.zip file into the container
COPY ./main.zip /app/

# Copy entrypoint script
COPY entrypoint.sh /app/

# Ensure the entrypoint script is executable
RUN chmod +x /app/entrypoint.sh

# Unzip the main.zip file into the /app directory
RUN unzip -o /app/main.zip -d /app/

# Install Python dependencies from requirements.txt inside main.zip

ENTRYPOINT ["/app/entrypoint.sh"]
