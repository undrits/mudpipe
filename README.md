# mudpipe

Parallelize UDPipe processing with Python Multiprocessing. 


Requires: UDPipe, xz, xzcat, UDPipe model

Example:
```shell
 ./mudpipe.py \
        --input-xz \
        --output-xz \
        --arg='--tokenize' \
        --arg='--tag' \
        --arg='--parse' \
        --model=[some_model].udpipe \
        --path_dir=[data_dir].*.txt.xz
        --workers=4
```


# udpipe
UDPipe is developed by Institute of Formal and Applied Linguistics. For more information on UDPipe please see [here](https://ufal.mff.cuni.cz/udpipe).

# authors
Yulia Spektor & Kyle Gorman