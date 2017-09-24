
task SizeArray {
  Array[File] files

  command <<<
    python3 <<CODE

    import os
    from math import ceil

    files = [${sep=',' files}] # read array of files into a python list literal

    # add up sizes, convert to gb
    size = sum(os.stat(f).st_size for f in files)
    size = ceil(size / 1e9)

    # print the result (picked up by output)
    print(str(size))

    CODE
  >>>

  output {
    Int size = read_int(stdout())
  }


}
