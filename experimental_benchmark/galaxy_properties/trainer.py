from lightning.pytorch.cli import ArgsType, LightningCLI

def provabgs_cli(args: ArgsType = None, run: bool = True):
    return LightningCLI(
        args=args,
        run=run,
    )

if __name__ == "__main__":
    provabgs_cli(run=True)
