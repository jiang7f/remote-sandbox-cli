FROM ubuntu:22.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openssh-server python3 rsync tar ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /run/sshd && \
    useradd --create-home --shell /bin/bash test && \
    echo 'test:test-password' | chpasswd && \
    mkdir -p /home/test/.ssh /fixture && \
    chown -R test:test /home/test && \
    chmod 700 /home/test/.ssh

COPY sshd_config /etc/ssh/sshd_config

EXPOSE 2222
CMD ["sh", "-c", "install -m 600 -o test -g test /fixture/client.pub /home/test/.ssh/authorized_keys && exec /usr/sbin/sshd -D -e -f /etc/ssh/sshd_config"]
