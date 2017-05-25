FROM centos:6.7
MAINTAINER vcheng@pivotal.io
RUN yum -y update
RUN yum -y install \
    postgresql \
    systemtap \
    git \
    passwd
RUN /usr/sbin/groupadd gpadmin \
&& /usr/sbin/useradd gpadmin -g gpadmin -G wheel \
&& usermod -aG stapdev gpadmin \
&& echo "gpadmin        ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers\
&& chown -R gpadmin: /home/gpadmin \
&& echo -e "gpcc2017\ngpcc2017\n" | passwd gpadmin
RUN yum -y install \
    sudo 
USER gpadmin
WORKDIR /home/gpadmin
CMD exec $SHELL
