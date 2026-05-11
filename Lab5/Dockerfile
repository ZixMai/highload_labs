FROM apache/nifi:2.9.0

USER root

COPY nifi-start-with-local-flow.sh /opt/nifi/scripts/nifi-start-with-local-flow.sh

RUN mkdir -p \
    /opt/nifi/nifi-current/conf \
    /opt/nifi/nifi-current/state \
    /opt/nifi/nifi-current/content_repository \
    /opt/nifi/nifi-current/flowfile_repository \
    /opt/nifi/nifi-current/provenance_repository \
    /opt/nifi/nifi-current/database_repository \
    /opt/nifi/nifi-current/logs \
    /opt/nifi/nifi-current/extensions \
 && chmod 755 /opt/nifi/scripts/nifi-start-with-local-flow.sh \
 && chown -R nifi:nifi /opt/nifi/nifi-current /opt/nifi/scripts/nifi-start-with-local-flow.sh

USER nifi
