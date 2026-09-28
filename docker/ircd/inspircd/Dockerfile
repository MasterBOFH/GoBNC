# InspIRCd for the ircdtest matrix: the official image, pinned by tag and
# digest, with its stock config (which already sets useident="no") changed
# in four places. Clients listen on the image's default port 6667; compose
# publishes it as 4448.
FROM inspircd/inspircd-docker:4.12.1@sha256:932f9d6de542e8084cdfd62e28a80c329d402a88d46dfdaf2ee95c841d747540

USER root
# - localmax/globalmax 3 -> 20: tests open several connections at once from
#   the one Docker gateway address.
# - connectban off: it Z-lines an IP after 10 connections an hour, which
#   repeated test runs against one container would trip.
# - resolvehostnames off: the reverse lookup of the Docker gateway goes to
#   the host's upstream DNS; once that stops answering (seen after a burst
#   of test connections), every registration waits out the 5s DNS timeout.
# The greps fail the build if the stock config ever stops matching.
RUN sed -i -e 's/localmax="3"/localmax="20"/' -e 's/globalmax="3"/globalmax="20"/' \
        -e 's/^\( *\)resolvehostnames="yes"/\1resolvehostnames="no"/' /conf/inspircd.conf \
 && sed -i 's|^<module name="connectban">|#<module name="connectban">|' /conf/modules.conf \
 && grep -q 'localmax="20"' /conf/inspircd.conf \
 && grep -q 'globalmax="20"' /conf/inspircd.conf \
 && grep -q '^ *resolvehostnames="no"' /conf/inspircd.conf \
 && grep -q '^#<module name="connectban">' /conf/modules.conf
USER inspircd

# DNSBL lookups would query real blacklists on every test connection.
ENV INSP_ENABLE_DNSBL=no
# Otherwise the server names itself after the container ID.
ENV INSP_SERVER_NAME=inspircd.test
