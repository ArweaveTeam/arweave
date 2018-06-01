%%% A collection of record structures used throughout the Arweave server.

%% @doc How nodes identify they are on the same network.
-define(NETWORK_NAME, "arweave.N.1").

%% @doc Current release number of the arweave client software.
-define(CLIENT_VERSION, 3).

%% @doc Specifies whether the software should be run in debug mode
%% (excuting ifdef code blocks).
%% WARNING: Only define debug during testing.
% -define(DEBUG, debug).

%% @doc Default auto-update watch address.
-define(DEFAULT_UPDATE_ADDR, "8L1NmHR2qY9wH-AqgsOmdw98FMwrdIzTS5-bJi9YDZ4").

%% @doc Should ar:report_console/1 /actually/ report to the console?
-define(SILENT, true).

%% @doc The hashing algorithm used to calculate wallet addresses
-define(HASH_ALG, sha256).

%% @doc The hashing algorithm used to verify that the weave has not been
%% tampered with.
-define(MINING_HASH_ALG, sha384).
-define(HASH_SZ, 256).
-define(SIGN_ALG, rsa).
-define(PRIV_KEY_SZ, 4096).

%% @doc NB: Setting the default difficulty high will cause TNT to fail.
-define(DEFAULT_DIFF, 8).

-ifndef(TARGET_TIME).
-define(TARGET_TIME, 120).
-endif.

-ifndef(RETARGET_BLOCKS).
-define(RETARGET_BLOCKS, 10).
-endif.

-define(RETARGET_TOLERANCE, 0.1).
-define(BLOCK_PAD_SIZE, (1024*1024*1)).

%% @doc The total supply of tokens in the Genesis block,
-define(GENESIS_TOKENS, 55000000).

%% @doc Winstons per AR.
-define(WINSTON_PER_AR, 1000000000000).

%% The base cost of a byte in AR
-define(BASE_BYTES_PER_AR, 10000000).

%% Base wallet generation fee
-define(WALLET_GEN_FEE, 250000000000).

%% @doc The minimum cost per byte for a single TX.
-define(COST_PER_BYTE, (?WINSTON_PER_AR div ?BASE_BYTES_PER_AR)).

%% The difficulty "center" at which 1 byte costs ?BASE_BYTES_PER_AR
-define(DIFF_CENTER, 40).

%% @doc The amount of the weave to store. 1.0 = 100%; 0.5 = 50% etc.
-define(WEAVE_STORE_AMT, 1.0).

%% @doc The number of blocks behind the most recent block to store.
-define(STORE_BLOCKS_BEHIND_CURRENT, 50).

%% Speed to run the network at when simulating.
-define(DEBUG_TIME_SCALAR, 1.0).

%% @doc Length of time to wait before giving up on test(s).
-define(TEST_TIMEOUT, 5 * 60).

%% @doc Calculate MS to wait in order to hit target block time.
-define(DEFAULT_MINING_DELAY,
    ((?TARGET_TIME * 1000) div erlang:trunc(math:pow(2, ?DEFAULT_DIFF - 1)))).

%% @doc The maximum size of a single POST body.
-define(MAX_BODY_SIZE, 3 * 1024 * 1024).

%% @doc Default timeout value for network requests.
-define(NET_TIMEOUT, 300 * 1000).

%% @doc Default timeout value for local requests
-define(LOCAL_NET_TIMEOUT, 10000).

%% @doc Default timeout for initial request
-define(CONNECT_TIMEOUT, 10 * 1000).

%% @doc Default time to wait after a failed join to retry
-define(REJOIN_TIMEOUT, 3000).

%% @doc The amount of time to wait before refreshing miner data in case of a
%% difficulty change.
-define(REFRESH_MINE_DATA_TIMER, 60000).

%% @doc Time between attempts to find optimise peers.
-define(GET_MORE_PEERS_TIME,  240 * 1000).

%% @doc Time to wait before not ignoring bad peers.
-define(IGNORE_PEERS_TIME, 5 * 60 * 1000).

%% @doc Number of transfers for which not to score (and potentially drop)
%% new peers.
-define(PEER_GRACE_PERIOD, 100).

%% @doc Never drop to lower than this number of peers.
-define(MINIMUM_PEERS, 4).

%% @doc Never have more than this number of peers (new peers excluded).
-define(MAXIMUM_PEERS, 20).

%% @doc Amount of peers without a given transaction to send a new transaction to.
-define(NUM_REGOSSIP_TX, 20).

%% Maximum nunber of requests allowed by an IP in any 30 second period.
-define(MAX_REQUESTS, 450).

%% @doc Delay before mining rewards manifest.
-define(REWARD_DELAY, ?BLOCK_PER_YEAR/4).

%% @doc List of default peers to connect
-define(DEFAULT_PEER_LIST, []).

%% @doc Peers to never add to peer list
-define(PEER_PERMANENT_BLACKLIST,[]).

%% @doc Length of time to wait (seconds) before dropping after last activity
-define(PEER_TIMEOUT, 480).

%% @doc Log output directory
-define(LOG_DIR, "logs").

%% @doc Port to use for cross-machine message transfer.
-define(DEFAULT_HTTP_IFACE_PORT, 1984).

%% @doc Number of mining processes to spawn
%% For best mining, this is set to the number of available processers minus 1.
%% More mining can be performed with every core utilised, but at significant
%% cost to node performance.
-define(NUM_MINING_PROCESSES, max(1, (erlang:system_info(schedulers_online) - 1))).

%% @doc Target number of blocks per year.
-define(BLOCK_PER_YEAR, 525600/(?TARGET_TIME/60) ).

%% @doc A block on the weave.
-record(block, {
	nonce = <<>>, % The nonce used to satisfy the mining problem when mined
	previous_block = <<>>, % indep_hash of the previous block in the weave
	timestamp = ar:timestamp(), % Unix time of block discovery
	last_retarget = -1, % Unix timestamp of the last difficulty retarget
	diff = ?DEFAULT_DIFF, % Puzzle difficulty, number of preceeding zeroes.
	height = -1, % How many blocks have passed since the Genesis block?
	hash = <<>>, % A hash of this block, the previous block and the recall block.
	indep_hash = [], % A hash of this block JSON encoded.
	txs = [], % A list of transaction records associated with this block.
	hash_list = undefined, % A list of all previous indepp hashes.
	wallet_list = [], % A map of wallet balances, or undefined.
    reward_addr = unclaimed, % Address to credit mining reward or unclaimed.
    tags = [], % Miner specified tags to store with the block.
	reward_pool = 0, % Current pool of mining reward.
	weave_size = 0, % Current size of the weave in bytes (counts tx data fields).
	block_size = 0 % The size of the transactions inside this block.
}).

%% @doc A transaction, as stored in a block.
-record(tx, {
	id = <<>>, % TX UID (Signature hash).
	last_tx = <<>>, % Get last TX hash.
	owner = <<>>, % Public key of transaction owner.
	tags = [], % Indexable TX category identifiers.
	target = <<>>, % Address of target of the tx.
	quantity = 0, % Amount of Winston to send.
	data = <<>>, % Data in transaction (if data transaction).
	signature = <<>>, % Transaction signature.
	reward = 0 % Transaction mining reward.
}).

%% @doc Gossip protocol state.
%% Passed to and from the gossip library functions (ar_gossip).
-record(gs_state, {
	peers, % A list of the peers known to this node.
	heard = [], % Hashes of the messages received thus far.
	loss_probability = 0, % Message loss probability for network simulation.
	delay = 0, % Message passing delay for network simulation.
	xfer_speed = undefined % Transfer speed in bytes/s for network simulation.
}).

%% @doc A message intended to be handled by the gossip protocol
%% library (ar_gossip).
-record(gs_msg, {
	hash,
	data
}).

%% @doc A record to describe a known Arweave network service.
-record(service, {
	name,
	host,
	expires
}).

% @doc A record to define HTTP Performance results for a given node.
-record(performance, {
	bytes = 0,
	time = 0,
	transfers = 0,
	timestamp = 0,
	timeout = os:system_time(seconds)
}).

%% @doc A Macro to return number of winstons per given AR.
-define(AR(AR), (?WINSTON_PER_AR * AR)).

%% @doc A Macro to return whether an object is a block.
-define(IS_BLOCK(X), (is_record(X, block))).

%% @doc A Macro to return whether a value is an address.
-define(IS_ADDR(Addr), (is_binary(Addr) and (bit_size(Addr) == ?HASH_SZ))).

%% The messages to be stored inside the genesis block.
-define(GENESIS_BLOCK_MESSAGES,
[
	{"The line dividing good and evil cuts through the heart of every human being."},
	{"Jon, there are no words."},
	{	
		"Jon, you were a part of all my best adventures. The memories and joy of all of these thousands of moments is intertwined with you, who you are to me, and how special you are as a person. You were like a brother to me, a best friend too, and I miss you dearly every day. Every future adventure I have, every time I fall in love with a new album, film, book or recipe, I wish I could share it with you. Your friendship changed my life, and so has your passing. I’m so pleased and thankful to have known you and so devastated to have lost you so quickly. \n
		\n
		You were truly unique, you were always such stimulating company, always making me laugh and think in equal measure. Whenever I spoke to you I learnt something new. You always wanted to share and create knowledge together, and I much admired you for this reason among countless others. Just as you are totally irreplaceable, you’re totally unforgettable, and in this way you will always be with those of us who love you.\n
		\n
		I miss you Jon, and I wish I could give you just one last big hug.\n
		\n
		Love,\n
		India"
	},
	{
		"Your learning, your wit, your forgetfulness, your determination, your presence of mind, your absence of mind, your kindness, your generosity of spirit and maddening indecision, your big feet and bigger heart, your confidence and doubt and painful sensitivity, your self-righteous hectoring and lecturing, your terrible knowledge and innocent ignorance.\n
		\n
		Your mumbling, bumbling, grumbling and sleepy-eyed stumbling. Your bluntness and sharpness.\n
		\n
		Your disapproval of my writing this.\n
		\n
		The stopped clocks of those who loved you, the meaningless everyday, the dull ache of hearts slowly breaking......It was so nice holding your hand for a while.\n
		\n
		The piece of me that made you, what was it that unmade you?\n
		\n
		A part of me goes with you. ( Look after it for me)\n
		\n
		Mum & Dad.\n
		xx"
	},
	{
		"Jonathan Eliot Sherry\n
		22.6.93 – 12.1.18\n
		\n
		Jon,\n
		You added something special to the lives of all who knew you and your leaving has taken something special away. We are\n
		no longer quite as whole as we were. We miss you every single minute of every single day.\n
		We will love you forever and you will always be with us in our hearts and thoughts.\n
		Mum and Tony"
	},
	{
		"Jon was a part of all my best adventures. The memories and joy of all of these thousands of moments is intertwined with Jon, who he was to me, and how special he was as a person. Jon was like a brother to me, a best friend too, and I miss him dearly every day. Every future adventure I have, every time I fall in love with a new album, film, book or recipe, I wish I could share it with Jon. Jon’s friendship changed my life, and so has his passing. I’m so pleased and thankful to have known him and devastated to have lost him so quickly.\n
 
		Jon was truly unique, he was always stimulating company, always making you laugh and think in equal measure. Whenever I spoke to Jon I learnt something new. Jon always wanted to share knowledge and create together, and I much admired him for this reason among countless others. Just as Jon is totally irreplaceable, he is totally unforgettable, and in this way he will always be with those of us who loved him.\n

		I miss you Jon, and I wish I could give you just one last big hug. \n

		Love,\n
		India"
	},
	{
		"Jon it was a joy to have known you, your witty comments and small outbursts lit the office, at least to me.

		I still remember the time we went out for lunch and got a 'romantic' booth for two by the window and finished off with
		a nice stroll through the park, those were the times.

		Take care buddy.
		Damon"
	},
	{"שְׁמַע יִשְׂרָאֵל יְהוָה אֱלֹהֵינוּ יְהוָה אֶחָֽד׃"},
	{"Сговорна дружина планина повдига!"},
	{"I love you"},
	{"So cool to buy archain"},
	{"I am sorry Josch. I'm afraid I can't do that."},
	{"Choose your joy; Master thyself. - J. Friedman"},
	{"BART & IVAI 2017"},
	{"Confident and Good  Luck !"},
	{"Namo Buddhaya"},
	{"The start of a new future for Dr and Dr Lakhoo, as our new child is due in 11 days - I mark this in the Internet Archive of the future :)"},
	{"You guys are an inspiration. I believe that this project will succeed!"},
	{"Takino Yumiko,"},
	{"For a future where our kids will remember you forever."},
	{"OOH WEE"},
	{"Are we there yet?"},
	{"High risk"},
	{"high reward."},
	{"Here we go :)"},
	{"If God is for us"},
	{"who can be against us?"},
	{"Blub"},
	{"Regularguy was here"},
	{"Livelaughlove "},
	{"Mæf - kebab to the people"},
	{"Damian Lewis ❤"},
	{"How do you stop revisionist history? Create an immutable, freely distributed and decentralized archive. - The Faculak's"},
	{"Best of luck"},
	{"guys!"},
	{"I love cincillà"},
	{"test"},
	{"I hope senpai will notice me after i buy this."},
	{"loferbart2"},
	{"testing"},
	{"6"},
	{"Who controls the past controls the future."},
	{"#igrokinvestor"},
	{"when you're reading this in 2030 i'll be richer than a rothschild"},
	{"@APurpleKoala"},
	{"test"},
	{"AppCedar - Paving a Way"},
	{"Glad to be on board!"},
	{"Gotta love Chains!"},
	{"Don't tell Marlee"},
	{"LEEROY JENKINS"},
	{"Let's see"},
	{"looking forward to this!"},
	{"A Trying Man"},
	{"test2"},
	{"I really wanted to measure Sonu that day! missed chance! :0-) "},
	{"Mali Paštetek"},
	{"Hope Arcchain does great!"},
	{"Andrew Rudman"},
	{"Keep on climbing!"},
	{"I hope this doesn't cause me any trouble."},
	{"I really belive that Internet is in a disruptive moment. sep/2017"},
	{"To be AND not to be = Love"},
	{"Keen to support this venture"},
	{"Good luck Sam and team."},
	{"hackg was here"},
	{"Lord Patrick of The Castle Hamburg"},
	{"hebikvandaagalgezegthoeveelikvanjehou. RR-MK-K-E en Roetie"},
	{"I buy these coins for Jackie"},
	{"my best friend and soon to be my wife. My our future be bright and shiny!"},
	{"Think Not what the Internet can do for you, Think what You can do for the Intenet."},
	{"MR J was here"},
	{"Craig Galloway from South Africa backs this "},
	{"Hello"},
	{"I'd like to contribute :)"},
	{"Finis coronat opus!"},
	{"test"},
	{"My boys"},
	{"Let the history say that i believed in this project!"},
	{"This is the fucking future."},
	{"Good luck - Archain"},
	{"Love and Peace! JZ was here."},
	{"Hi :) This will be around in 100 years still. Pretty Cool. SPH. CY. "},
	{"2"},
	{"1"},
	{"Goodluck!"},
	{"Cheers!"},
	{"This is a test"},
	{"Kamilla er sød!"},
	{"I love you Sanah Mian"},
	{"Sammen for Drammen"},
	{"In on history."},
	{"World Peace."},
	{"weasel"},
	{"Smooch buddies Kumirei and Abertssquirrel were here. "},
	{"For everyone's future.  May we all live a peaceful and prosperous life!,"},
	{"PLS SUCCEED"},
	{"Philip Dunay"},
	{"ameliacorreacano"},
	{"test"},
	{"#MAGA! "},
	{"Good luck"},
	{"Looks exciting"},
	{"Willing to bet on this becoming huge!!"},
	{"I am from the past!"},
	{"Godspeed UnZane"},
	{"test"},
	{"great product"},
	{"Spread love."},
	{"My first ICO!"},
	{"Investing into those I believe and trust in.......Step x"},
	{"God is good"},
	{"JUSTICE!"},
	{"don't confuse motion with progress"},
	{"Alee invest ARC"},
	{"Good Luck"},
	{"test"},
	{"samdowney.com"},
	{"I believe in your business"},
	{"I am e99y."},
	{"Maxime Courgibet"},
	{"3"},
	{"Foundation just begun!"},
	{"Here's to the future!"},
	{"StillArts"},
	{"test"},
	{"Good luck for your nice project"},
	{"Git Good"},
	{"Hello world :-)"},
	{"Together we shall witness and guard the history of mankind. AG."},
	{"The misery that is now upon us is but the passing of greed\n
	The bitterness of men who fear the way of human progress\n
	From perfect grief there need not be\n
	Wisdom or even memory:\n
	One thing then learnt remains to me,-\n
	The woodspurge has a cup of three.\n
	Ad maiora. Ad infinitum."},
	{"MMXVII"},
	{"test"},
	{"An uncensored web is integral to our political survival."},
	{"For Valeriia, Harry and Charlie Scholey. You are my everything."},
	{"IARWBTHPIASITUS"},
	{"IEIWGTMEPARTHQITCOASCAA"},
	{"SI2020IWAWFAFTOBTEO2030IWHIMPTMD"},
	{"IWLTWIPAAIHAH"},
	{"Can't wait for archain to become unchained...💪"},
	{"To my sons future"},
	{"Supporting Archain and the future! "},
	{"hello"},
	{"Batman likes you - Upendra Meena"},
	{"Grow Archain grow"},
	{"My trust in this project is reflected by my contributions. "},
	{"eminem indian fan!"},
	{"Those who do not remember the past are condemned to repeat it."},
	{"This sounds like it could make someone very powerful very angry"},
	{"I'm in."},
	{"Suchet SD - remember my name"},
	{"Great concept"},
	{"Good luck!"},
	{"And on the pedestal these words appear:\n
		My name is Ozymandias\n
		king of kings:,\n
		Look on my works,\n
		ye Mighty,\n
		and despair!'"},
	{"Nothing beside remains. Round the decay"},
	{"Of that colossal wreck"},
	{"boundless and bare"},
	{"The lone and level sands stretch far away."},
	{"Time to re-read the Voyager 1 golden record."},
	{"Good luck!"},
	{"Yeah! Dokuments!"},
	{"test"},
	{"Foutight loves Mini-Brig"},
	{"Удачной реализации концепции!"},
	{"CryptosRUs.com supports Archain"},
	{"Aum Gam Ganapataye Namaha Aum Im Hreem Sreem SreeMaathre Namaha Aum SriSaiRam GuruDeva Datta - VijayaDurga I "},
	{"TulasiKrishna I"},
	{"this sounds like a really promising project!"},
	{"Sucatemela Fortissimo"},
	{"gread"},
	{"Make some history!"},
	{"While I may not have been born early enough to explore unchartered lands or born late enough to explore deep space"},
	{"I feel privileged to have been granted the opportunity to have explored the internet during its rise."},
	{""},
	{"From Kris D"},
	{"free will"},
	{"no censorship"},
	{"exciting and new technology"},
	{"This is my second attempt to buy arc's "},
	{"test"},
	{"We'll see what happens"},
	{"I will always love you Scout and Avi"},
	{"Welcome to the new beginning!"},
	{"First try"},
	{"Hello future!"},
	{"Have a look at some words of the early adopters!"},
	{"Hello our future selves. We tried…"},
	{"io4"},
	{"Hello decentralized world from Evangelos Barakos!"},
	{"Invest in humanity and happiness"},
	{"Blessed are the meek"},
	{"Risk vs. Reward"},
	{"Welcome to the party"},
	{"guys!"},
	{"Congrats on ur launch"},
	{"This is Cynthia from SZ"},
	{"China."},
	{"Nice to meet u here"},
	{"It's interesting to meet somebody at this plarform"},
	{"so let me know if u want to talk with me for some new things"},
	{"Hi I'm Jason Being"},
	{"enlightened by way of the R+C. May the infinite blockchain bring LLL to all! So more it be!"},
	{"More Rigs"},
	{"More Coins"},
	{"More Life"},
	{"We don't own a business, But we mean business "},
	{"Hi guys"},
	{"All the best with the project!"},
	{"Greetings."},
	{"Hey. See you when the price hits 1000$ per token :)"},
	{"Indie and Sam "},
	{"So excited for you ... so proud!"},
	{"Love you"},
	{"Mum / Philippa xxx"},
	{"test"},
	{"For my wife Pricilla and my unborn daughter, Amber. I love you!"},
	{"I throw my coin into this wishing well. good luck to this new currency."},
	{"Hello World"},
	{"just made my 1st ico purchase."},
	{"Good Luck"},
	{"Stranger in a strange land. Land of ice and snow. Trapped inside this prison. Lost and far from home."},
	{"HODLETHEREUMBABY"},
	{"Don't waste your time on jealousy. Sometimes you're ahead sometimes you're behind; the race is long but in the end it's only with yourself."},
	{"ROCK SOLID PROJECT"},
	{"Rak"},
	{"Clara"},
	{"Marta y Papi forever!!"},
	{"best of luck"},
	{"Adam Shir supports building the future decentralized society for all people, nations and generations. "},
	{"2"},
	{"test"},
	{"Dedicated to my sons"},
	{"arcsupport"},
	{"This is very exciting and I wholeheartedly support this project :)"},
	{"Nihil sine Deo!"},
	{"Monkey Capital with Daniel Harrison is a crypto scam group. "},
	{"willwuzhere-09/17"},
	{"I'll go to space travel!"},
	{"Never pretend to a love which you do not actually feel, for love is not ours to command."},
	{"game."},
	{"transparency and integrity are the salvation of humanity.  "},
	{"The 1 & Only Dogstartaylor"},
	{"I'm really excited about this"},
	{"trenchantent"},
	{"&#65533;�유"},
	{"&#65533;�연&#65533;&#65533; &#65533;�랑&#65533;&#65533;."},
	{"&#65533;&#65533;�� &#65533;�복&#65533;�자! "},
	{""},
	{"lets take over the world!!!"},
	{"KAOS REIGNS"},
	{"order is nothing more than a characteristic of the human inability to accept chaos."},
	{"I'm in!! this is game changing."},
	{"ROCK SOLID"},
	{"Gimme the ARChain! I think the world needs it and I hope it will happen."},
	{"Good Luck Archain!"},
	{"I love you Kelsey. Here's to a world where our future children can live in peace and prosperity. "},
	{"@benjbrandall"},
	{"this really has some great potential."},
	{"You make my floppy drive hard."},
	{"Great Idea"},
	{"good luck!"},
	{"Hi first time contributor . I have a hard Ike understanding any of it but what I do grasp "},
	{"I like . A lot. Cheers"},
	{"Hello"},
	{"Hey Archain!! Santosh here"},
	{"kashey@archive.lib"},
	{"Amo Giovanna Zippone"},
	{"internet forever"},
	{"asd"},
	{"test"},
	{"Karmapa chenno"},
	{"Julio Saavedra"},
	{"I can't wait to see where this goes! All the best"},
	{"sonatix.com.ua love everyone :D"},
	{"Dan Andrews and Dominik Schiener (of IOTA) must be brothers. They look identical. Search it."},
	{"🙈"},
	{"sonatix"},
	{"To my loving wife Cara"},
	{"my two beautiful boys Zach"},
	{"Jonah"},
	{"and soon to be Noah and possibly any other kid(s)/grand/great-grand etc. that we might have. I love you all with all my heart. Nothing could ever change that. I hope one day every child on earth can feel this love."},
	{"jathin menon c/o uroob was here"},
	{"Let freedom prevail!"},
	{"I expect this project!(@nils00000)"},
	{"Hector Recio Molina"},
	{"Fantastic idea son! Loving the digital adventures you are taking me on! Love always your proud mum X..."},
	{"Benedikte BS - jeg elsker deg<3"},
	{"I love Beth Stein"},
	{"La censura non ci fermera'!"},
	{"The egg is hatched. The seed has grown."},
	{"The sands of time that slowly flow throughout my hourglass"},
	{"will all too soon have embeded away and my life will then have past. So I must make the most of time drifting not with the tides"},
	{"for killing time is not murder it's more like suicide!"},
	{"This Is For My Sons Future! The K.I.N.G Gilroy was Here!"},
	{"I am Matthew"},
	{"son of Ed and father of Dirk. August 26, 2017."},
	{"Panka & Kende"},
	{"Remember Bitcoins? If so, send your spares to this address, since they're now obsolete! 1eZr2EfVhdBfpF2JHrVTrAHsgHgywWo4D"},
	{"RG08"},
	{"digamma889 was here..."},
	{"Sullof was here :-)"},
	{"Investing in FREEDOM"},
	{"Karthik's Birthday"},
	{"Then and there here and now. Dissolve into cosmic oneness."},
	{"lets see this work"},
	{"test"},
	{"Good luck archain! Great project. Hope this is the start of the new internet. Love to my children Oliver N and Deirdre O and lady Lisa xx"},
	{"Because some things should never be forgotten "},
	{"4"},
	{"why not"},
	{"Welcome to the future!"},
	{"hi"},
	{"Not to feel exasperated"},
	{"or defeated"},
	{"or despondent because your days aren't packed with wise and moral actions. But to get back up when you fail"},
	{"to celebrate behaving like a human--however imperfectly--and fully embrace the pursuit that you've embarked on. - Marcus Aurelius"},
	{"Johnny Test!"},
	{"Lord\n
	make me an instrument of Thy peace;\n
	where there is hatred\n
	let me show love;\n
	where there is injury\n
	pardon;\n
	where there is doubt\n
	faith;\n
	where there is despair\n
	hope;\n
	where there is darkness\n
	light;\n
	and where there is sadness\n
	joy.\n
	O Divine Master,\n
	grant that I may not so much seek to be consoled as to console;\n
	to be understood\n
	as to understand;\n
	to be loved\n
	as to love;\n
	for it is in giving that we receive,\n
	it is in pardoning that we are pardoned,\n
	and it is in dying that we are born to Eternal Life."},
	{"_.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-.__.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._.-._ XOXO Callerblod"},
	{"This is a great business model and I'm so excited to be a part. "},
	{"Julio Cesar Saavedra"},
	{"MONEY'S WORTH\n,

	When your God comes a calling\n
	and you face up to him.\n
	Will you then confess\n
	to man's greatest sin?\n
	The confession of mankind\n
	to God you now must tell,\n
	Lets hope that he's forgiving\n
	for we may all burn in hell.\n
	\n
	Bless me lord for I have sinned\n
	I lost the meaning of life,\n
	and everywhere I travelled\n
	I held a bloody knife.\n
	Who's is the knife? Who's is the blood? Don't know but they're not mine,\n
	you have not got the bottle\n
	but you paid and that's your crime.\n
	\n
	The beauty of the silverback\n
	with its great big ashtray hands. \n
	Crush a rhino's horn\n
	for medicine in far off lands.\n
	The biggest of all the cats\n
	you've a tiger for a rug,\n
	and for it's ivory tusks\n
	king elephant you will mug. \n
	\n
	They're only bloody animals! Who cares about the beasts?\n
	When they're all dead and gone\n
	we'll move on to bigger feasts.\n
	Take all the goodness from his land\n
	and yes you will endeavour.\n
	To keep a fellow man enslaved\n
	and diamonds are forever.\n
	\n
	Man's mean machine keeps rolling\n
	now which land will it spoil?\n
	He's got to feed his industry\n
	his land it has no oil.\n
	So he sets a brother against his brother\n
	they say that's what they need.\n
	The starving winners promised food\n
	whilst oil feeds corporate greed.\n
	\n
	When a human being is born\n
	it's vulnerable and nude.\n
	Just like any other animal\n
	It only thinks of food,\n
	but as a human grows\n
	and this I don't find funny,\n
	it's taught to worship\n
	honour\n
	love and kill for money.\n
	Live progressive\n
	stay minimal. Afm\n
	Luck and love to all! \n
	be good\n
	do good"}
]).

