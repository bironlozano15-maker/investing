module.exports = {
  apps: [
    {
      name: "investing_staking1",
      interpreter: "python3",
      script: "./neurons/miner.py",
      args: "--netuid 88 --logging.debug --logging.trace --wallet.name bt --wallet.hotkey live1",
      env: {
        PYTHONPATH: ".",
      },
    },
  ],
};
