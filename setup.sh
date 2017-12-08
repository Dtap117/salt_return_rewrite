yum install mysql-devel
curl https://bootstrap.pypa.io/ez_setup.py -o - | python
easy_install pip
pip install -r requirements.txt
cd src && sh control restart
