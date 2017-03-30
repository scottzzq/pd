kill -9 `ps x | grep pd-server | grep -v grep | awk '{print $1}'`
