# Stage 1: Build Environment
FROM buildpack-deps AS build-stage
RUN ["apt", "update"]
RUN ["apt", "install", "-y", "cmake", "dos2unix"]
ADD "./Unicycler" "/opt/Unicycler"
WORKDIR "/opt/Unicycler"
RUN make
RUN ["dos2unix", "unicycler-runner.py"]

ADD "./racon" "/opt/racon"
WORKDIR "/opt/racon/build"
RUN ["cmake", "-DCMAKE_BUILD_TYPE=Release", ".."]
RUN make

WORKDIR "/opt"
RUN wget https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/2.16.0/ncbi-blast-2.16.0+-x64-linux.tar.gz
RUN tar -xzf ncbi-blast-2.16.0+-x64-linux.tar.gz
RUN wget https://github.com/ablab/spades/releases/download/v4.0.0/SPAdes-4.0.0-Linux.tar.gz
RUN tar -xzf SPAdes-4.0.0-Linux.tar.gz
# Stage 2: Runtime environment
FROM python:3.10.14 AS final-stage
#  Copy application artifacts from the build stage (e.g., JAR file)
COPY --from=build-stage "/opt/Unicycler" "/opt/Unicycler"
COPY --from=build-stage "/opt/racon/build/bin" "/opt/racon/bin"
COPY --from=build-stage "/opt/ncbi-blast-2.16.0+/bin" "/opt/ncbi-blast/bin"
COPY --from=build-stage "/opt/SPAdes-4.0.0-Linux" "/opt/SPAdes"

RUN ["ln", "-fs", "/opt/Unicycler/unicycler-runner.py", "/usr/local/bin/unicycler"]
ENV PATH="$PATH:/opt/racon/bin:/opt/ncbi-blast/bin:/opt/SPAdes/bin"
# Define runtime configuration (e.g., CMD, ENTRYPOINT)
