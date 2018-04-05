set arg1=%*
mkdir ebin logs blocks wallets txs

:mine
	erlc +export_all -o ebin/ src/ar.erl
	erl -pa ebin/ -s ar rebuild -run ar main %arg1%	
	if %ERRORLEVEL%==0 (
		echo "Heartbeat: Server terminated safely."
		pause
		exit 1
	) else (
		echo "Heartbeat: The Arweave server has terminated unsafely. It will restart in 15 seconds."
		echo "Heartbeat: If you would like to avoid this, please press control+C now..."
		timeout 15
	goto mine
	)

pause
