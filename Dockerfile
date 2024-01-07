FROM quay.io/astronomer/astro-runtime:10.0.0
USER root
RUN cd /tmp && \
    wget -qO- https://get.nextflow.io | bash && \
    mv nextflow /usr/bin/ && \
    chmod a+rx /usr/bin/nextflow && \
    cd -
USER astro