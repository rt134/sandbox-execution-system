FROM ubuntu/squid:5.2-22.04_beta@sha256:8b4566d582f3c7333a59d8aa370e7a2df255a5b51239c0716616334a1599557a

COPY squid.conf /etc/squid/squid.conf

EXPOSE 3128

ENTRYPOINT ["squid", "-N", "-d", "1"]
