FROM debian:trixie-slim

RUN apt-get update && apt-get upgrade -y && \
    apt-get -y install iptables iproute2 ulogd2

COPY run.sh /usr/bin/
RUN chmod +x /usr/bin/run.sh

COPY ulogd.conf /etc/ulogd.conf

CMD ["run.sh"]
