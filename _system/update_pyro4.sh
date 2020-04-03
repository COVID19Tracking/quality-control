cp quality-control.service /etc/systemd/system/quality-control.service
systemctl daemon-reload
systemctl restart quality-control
systemctl status quality-control
