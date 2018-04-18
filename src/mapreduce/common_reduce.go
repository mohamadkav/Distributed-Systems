package mapreduce

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	
	var intermediates []KeyValue

	for m:= 0; m<nMap; m++ {
		fileName := reduceName(jobName, m, reduceTask)
		openFile, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		decoder := json.NewDecoder(openFile)
		end := false
		for end == false {
			var decoded KeyValue
			err = decoder.Decode(&decoded)
			if err != nil {
				end = true
			}
			intermediates = append(intermediates, decoded)
		}

		openFile.Close()
	}


	sort.Slice(intermediates, func(i, j int) bool {
		return strings.Compare(intermediates[i].Key, intermediates[j].Key) == -1
	})

	oFile, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	encoder := json.NewEncoder(oFile)
	var uniqueKeySlice []string
	prevKey := intermediates[0].Key
	var currKey string
	for _, kv := range intermediates {
		currKey = kv.Key
		if strings.Compare(prevKey,currKey) != 0 {
			encoder.Encode(KeyValue{prevKey, reduceF(prevKey, uniqueKeySlice)})
			uniqueKeySlice = uniqueKeySlice[:0]
		}
		uniqueKeySlice = append(uniqueKeySlice, currKey)
		prevKey = currKey
	}

	oFile.Close()
}
