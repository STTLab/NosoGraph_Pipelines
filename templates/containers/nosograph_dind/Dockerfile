FROM docker
ADD "./templates/containers/nosograph_assemblers" "/docker_files/nosograph_assemblers"
USER root
RUN dockerd
RUN docker build -t nosograph-assemblers:latest /docker_files/nosograph_assemblers
