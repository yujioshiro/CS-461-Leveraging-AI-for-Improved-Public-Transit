import argparse
import os


def create_parser(description, category, add_test=True):
    """Create parser with category-aware defaults"""
    parser = argparse.ArgumentParser(description=description)

    # Default paths based on category
    default_input = os.path.join("data", "sources", category, "raw")
    default_output = os.path.join("data", "sources", category, "processed")

    parser.add_argument(
        "--input-dir",
        default=default_input,
        help=f"Input directory (default: {default_input})"
    )
    parser.add_argument(
        "--output-dir",
        default=default_output,
        help=f"Output directory (default: {default_output})"
    )

    if add_test:
        parser.add_argument(
            "--test", action="store_true",
            help="Use test data directories instead"
        )

    return parser


def handle_paths(args, category):
    """Handle path logic with test mode override"""
    if args.test:
        base = os.path.join("tests", "data", "sources", category)
        args.input_dir = os.path.join(base, "raw")
        args.output_dir = os.path.join(base, "processed")

    # Ensure directories exist
    os.makedirs(args.input_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Using input directory: {args.input_dir}")
    print(f"Using output directory: {args.output_dir}")
    return args
