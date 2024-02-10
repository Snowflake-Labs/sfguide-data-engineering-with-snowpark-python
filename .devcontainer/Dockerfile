FROM mcr.microsoft.com/devcontainers/miniconda:0-3

USER vscode
WORKDIR /home/vscode

# Configure SnowSQL
RUN mkdir .snowflake
COPY .devcontainer/config.toml .snowflake
COPY .devcontainer/connections.toml .snowflake

# Create the conda environment
COPY environment.yml .
RUN conda env create \
    && conda init \
    && rm environment.yml