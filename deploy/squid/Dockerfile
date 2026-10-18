FROM ubuntu/squid:latest

COPY squid.conf /etc/squid/squid.conf

EXPOSE 3128

ENTRYPOINT ["squid", "-N", "-d", "1"]
