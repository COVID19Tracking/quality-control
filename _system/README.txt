service setup:

    sudo cp quality-control.service /etc/systemd/system/quality-control.service
    sudo systemctl daemon-reload
    sudo systemctl start quality-control
    sudo systemctl status quality-control

    journalctl -u quality-control.service -b
