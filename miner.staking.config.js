module.exports = {
  apps: [
    {
      name: "investing_staking",
      interpreter: "python3",
      script: "./neurons/miner.py",
      args: "--netuid 88 --logging.debug --logging.trace --wallet.name bt --wallet.hotkey live",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
