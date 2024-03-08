FROM mcr.microsoft.com/devcontainers/miniconda:0-3

USER vscode
WORKDIR /home/vscode

# Configure SnowSQL
RUN mkdir .snowflake
COPY --chown=vscode:vscode .devcontainer/config.toml .snowflake
COPY --chown=vscode:vscode .devcontainer/connections.toml .snowflake
RUN chmod 0600 .snowflake/config.toml \
    && chmod 0600 .snowflake/connections.toml

# Create the conda environment
COPY environment.yml .
RUN conda env create \
    && conda init \
    && rm environment.yml