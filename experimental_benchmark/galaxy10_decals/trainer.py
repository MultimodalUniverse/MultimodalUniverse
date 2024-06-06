from lightning.pytorch.cli import ArgsType, LightningCLI

def gz10_cli(args: ArgsType = None, run: bool = True):
    return LightningCLI(
        args=args,
        run=run,
    )

if __name__ == "__main__":
    gz10_cli(run=True)
