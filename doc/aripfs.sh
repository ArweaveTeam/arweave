#! /bin/bash

APIKEY="qwe123asd234zxc345"
SERVER="http://123.45.67.89:1984"

curlpost () {  # $1=hash to pin
	#echoerr $1
	path="/api/ipfs/getsend/"
	url="$SERVER$path"
	json="{\"api_key\":\"$APIKEY\", \"ipfs_hash\":\"$1\"}"
	/usr/bin/curl -H "Content-Type: application/json" -X POST -d $json $url
	echo 200
}

echoerr () {
	printf "%s\n" "$*" >&2
}

mapfile -t pins_done < pins.done
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

printf "%s\n" "${pins_new[@]}" > out.new
printf "%s\n" "${pins_curr_sorted[@]}" > out.curr
printf "%s\n" "${pins_done[@]}" > out.done

