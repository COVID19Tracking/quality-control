cp quality-control.site /etc/nginx/sites-available/quality-control
systemctl daemon-reload
systemctl restart nginx
systemctl status nginx
echo
echo "Enabled Sites:"
ls -l /etc/nginx/sites-enabled
