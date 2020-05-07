FROM  daocloud.io/centos:7

COPY gluster-client/libattr-2.4.46-13.el7.x86_64.rpm libattr-2.4.46-13.el7.x86_64.rpm
COPY gluster-client/attr-2.4.46-13.el7.x86_64.rpm attr-2.4.46-13.el7.x86_64.rpm
COPY gluster-client/psmisc-22.20-16.el7.x86_64.rpm psmisc-22.20-16.el7.x86_64.rpm
COPY gluster-client/glusterfs-libs-4.1.9-1.el7.x86_64.rpm glusterfs-libs-4.1.9-1.el7.x86_64.rpm
COPY gluster-client/glusterfs-fuse-4.1.9-1.el7.x86_64.rpm glusterfs-fuse-4.1.9-1.el7.x86_64.rpm
COPY gluster-client/glusterfs-client-xlators-4.1.9-1.el7.x86_64.rpm glusterfs-client-xlators-4.1.9-1.el7.x86_64.rpm 
COPY gluster-client/glusterfs-4.1.9-1.el7.x86_64.rpm glusterfs-4.1.9-1.el7.x86_64.rpm 
COPY gluster-rest /usr/bin/
RUN  rpm -ivh *.rpm  --force
RUN chmod +x /usr/bin/gluster-rest

EXPOSE 7030

ENTRYPOINT [ "/usr/bin/gluster-rest" ]
