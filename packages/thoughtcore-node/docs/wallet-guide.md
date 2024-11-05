# Set up Alias config

Go to the root directory of your computer

```sh
cd ~
```

Create a .profile file if missing

OR

Create a .bashrc if configuring for interactive Bash usage

```sh
touch .profile

OR

touch .bashrc
```

Edit the .profile file to insert:
> *Make sure to replace username*

```sh
alias thoughtmainnet='/Applications/Thought-Qt.app/Contents/MacOS/Thought-Qt -datadir=/Users/username/blockchains/thought-core/networks/mainnet/'

alias thoughtregtest='/Applications/Thought-Qt.app/Contents/MacOS/Thought-Qt -datadir=/Users/username/blockchains/thought-core/networks/regtest/'

alias thoughtcashmainnet='/Applications/ThoughtABC-Qt.app/Contents/MacOS/ThoughtABC-Qt -datadir=/Users/username/blockchains/thoughtcash/networks/mainnet/ -flexiblehandshake -initiatecashconnections'

alias thoughtcashregtest='/Applications/ThoughtABC-Qt.app/Contents/MacOS/ThoughtABC-Qt -datadir=/Users/username/blockchains/thoughtcash/networks/regtest/ -flexiblehandshake -initiatecashconnections'
```

Ensure Mongod is running

```sh
mongod
```

Start the Thoughtcore node in the /thoughtcore/ project root directory

```sh
npm run node
```

To run RegTest Thought Core RegTest Client

```sh
. ~/.profile
thoughtregtest
```

> If successful Thoughtcore logo should be blue and syncing blocks on mongod in the background

## How to Generate Blocks

Go to Help -> Debug Window -> console tab

Input generate command in the line to create 5000 Blocks

```sh
generate 5000
```

To find RegTest account address

```sh
getaccountaddress ""
```

Test transactions by sending to account address in the send tab

Check transaction results in the Transactions tab
