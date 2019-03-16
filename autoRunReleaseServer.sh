now=$(date +"%m_%d_%Y")
file="app_log/$now.log"
echo '[create file success]' + $file 

echo '[django start]'
python3 manage.py runserver 0.0.0.0:8080 >> $file 
# nohup python3 manage.py runserver 0.0.0.0:8890 &
echo '[django start]'
