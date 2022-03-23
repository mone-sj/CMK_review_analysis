import db, platform, socket

class cmn:
    today_path=db.today_path()
    osName=platform.system()
    hostName=socket.gethostname()