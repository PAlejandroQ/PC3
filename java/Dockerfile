# Use a Java base image
FROM openjdk:11

# Set the working directory inside the container
WORKDIR /app

# Copy the Java JAR file to the container
COPY almacenBackend.jar .

# Set the command to execute the JAR file with the provided arguments
CMD ["java", "-jar", "almacenBackend.jar"]

# Expose the necessary ports
EXPOSE 8080
EXPOSE 5672
