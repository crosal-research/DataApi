# boostap API app
#python3 -m API.user_api
#python3 -m API.main
python3 -m gunicorn.app.wsgiapp -c ./API/gunicorn.conf.py API.user_api:app
echo "Igniting the API..."
