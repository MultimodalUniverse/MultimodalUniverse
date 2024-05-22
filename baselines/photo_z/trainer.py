from lightning.pytorch.cli import ArgsType, LightningCLI

def photo_z_cli(args: ArgsType = None, run: bool = True):
    return LightningCLI(
        args=args,
        run=run,
    )

if __name__ == "__main__":
    photo_z_cli(run=True)
