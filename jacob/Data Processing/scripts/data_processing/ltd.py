from utils.ltd.core import LTDProcessor
from utils.general.args import create_parser, handle_paths


def process_ltd_data(input_dir: str, output_dir: str):
    """Main processing function"""
    processor = LTDProcessor(input_dir, output_dir)
    processor.process_files()


if __name__ == "__main__":
    # Configure argument parsing
    parser = create_parser("Process LTD Data", category="LTD")
    args = parser.parse_args()
    args = handle_paths(args, category="LTD")

    try:
        process_ltd_data(args.input_dir, args.output_dir)
        print(f"Successfully processed LTD data to {args.output_dir}")
    except Exception as e:
        print(f"Error processing LTD data: {str(e)}")
        raise
