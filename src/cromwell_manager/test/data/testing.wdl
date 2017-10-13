
task SleepAWhile {
  Int time

  command {
    lsblk
    df -k
    sleep ${time}
    echo "something"
  }

  runtime {
    cpu: "1"
    docker: "ubuntu:zesty"
    memory: "1 GB"
    disks: "local-disk 10 HDD"
  }
}

workflow Sleep {
  Int time

  call SleepAWhile {
    input:
      time = time
  }
}
