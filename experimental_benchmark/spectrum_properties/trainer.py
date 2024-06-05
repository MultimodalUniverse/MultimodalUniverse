from lightning.pytorch.cli import ArgsType, LightningCLI

def spectra_cli(args: ArgsType = None, run: bool = True):
    return LightningCLI(
        args=args,
        run=run,
    )

if __name__ == "__main__":
    spectra_cli(run=True)
