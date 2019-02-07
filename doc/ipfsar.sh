#! /bin/bash

APIKEY="test_key_ivan"
SERVER="http://178.62.126.142:1984"
PINS_DONE_FN="pins.done"

curlpost () {  # $1=hash to pin
	echoerr $1
	path="/api/ipfs/getsend/"
	url="$SERVER$path"
	json="{\"api_key\":\"$APIKEY\", \"ipfs_hash\":\"$1\"}"
	echoerr $json
	/usr/bin/curl -H "Content-Type: application/json" -X POST -d "$json" $url
	sleep .5s
	echo 200
}

echoerr () {
	printf "%s\n" "$*" >&2
}

#mapfile -t pins_done < $PINS_DONE_FN # linux / current bash
while IFS= read -r line; do pins_done+=("$line"); done < $PINS_DONE_FN # mac / old bash

pins_done_sorted=($(sort <<<"${pins_done[*]}"))

pins_curr=`ipfs pin ls | sort | awk '{print $1}'`
pins_curr_sorted=($(sort <<<"${pins_curr[*]}"))

# https://stackoverflow.com/questions/2312762/compare-difference-of-two-arrays-in-bash
pins_new=()
for i in "${pins_curr_sorted[@]}"; do
	skip=
	for j in "${pins_done_sorted[@]}"; do
		[[ $i == $j ]] && { skip=1; break; }
	done
	[[ -n $skip ]] || pins_new+=("$i")
done

for p in "${pins_new[@]}"; do
	response=$(curlpost $p)
	case $response in # 200 208 402
		200)
			pins_done+=("$p")
			;;
		208)
			pins_done+=("$p")
			;;
		402)
			break # ??
	esac
done

#printf "%s\n" "${pins_new[@]}" > pins.new
#printf "%s\n" "${pins_curr_sorted[@]}" > pins.curr
printf "%s\n" "${pins_done[@]}" > $PINS_DONE_FN
