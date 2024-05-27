from lightning.pytorch.cli import ArgsType, LightningCLI

def photo_class_cli(args: ArgsType = None, run: bool = True):
    return LightningCLI(
        args=args,
        run=run,
    )

if __name__ == "__main__":
    photo_class_cli(run=True)
