cp quality-flaskapp.service /etc/systemd/system/quality-flaskapp.service
systemctl daemon-reload
systemctl restart quality-flaskapp
systemctl status quality-flaskapp

