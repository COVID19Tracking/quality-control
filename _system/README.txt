Pyro4 service:
    runs and caches the check results for a minute

    > sudo update_pyro4.sh
    > journalctl -u quality-control.service -b

flask service:
    flask WCGI host

    > sudo update_flask.sh
    > journalctl -u quality-flaskapp.service -b


nginx service:
    web server

    > 
