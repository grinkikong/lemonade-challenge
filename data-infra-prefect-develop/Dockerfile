# Use the Prefect base image
FROM prefecthq/prefect:2-python3.11-kubernetes

# libGL shared library required by OpenCV (cv2) for image and video processing
USER root
RUN apt-get update && apt-get install -y libgl1 libglib2.0-0 && apt-get clean

# Install the specific version of Poetry
RUN pip install --upgrade pip
RUN pip install "poetry==1.8.2"
RUN poetry config virtualenvs.create false

# Set working directory
WORKDIR /opt/prefect/flows

# Copy the pyproject.toml and poetry.lock to install dependencies
COPY pyproject.toml poetry.lock /opt/prefect/flows/

# Connect to AWS CodeArtifact & Install project dependencies
RUN pip3 install awscli

# Install project dependencies using CodeArtifact token
RUN --mount=type=secret,id=CODEARTIFACT_TOKEN \
    POETRY_HTTP_BASIC_ARTIFACT_USERNAME=aws \
    POETRY_HTTP_BASIC_ARTIFACT_PASSWORD=$(cat /run/secrets/CODEARTIFACT_TOKEN) \
    poetry install --no-root --only main
