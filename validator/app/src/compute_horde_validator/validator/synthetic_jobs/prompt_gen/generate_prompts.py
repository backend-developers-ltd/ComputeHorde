# # Load model directly
import re
import datetime
import argparse
import collections
import torch
import ast
import io
import random
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
)

from seeds import *


class PromptGeneratingPrompt:
    def __init__(self):
        self.prompt_ending = " }}assistant"

    def random_select(self, arr: list[str], num: int = 5) -> str:
        random.shuffle(arr)
        return ", ".join(arr[:num]) + ", etc"

    def generate_prompt(self) -> str:
        num_prompts = random.choice([10, 15, 20, 25, 30])
        relevance_level = random.randint(5, 20)
        complexity_level = random.randint(5, 20)

        themes = self.random_select(THEMES, num=3)
        abilities = self.random_select(ABILITIES, num=4)
        formats = self.random_select(FORMATS, num=5)

        prompt = (
            f"Generate a list of {num_prompts} complex prompts (questions or instruct tasks) that cover a wide range of skills and knowledge areas related to the themes of {themes}. "
            # f"Generate a python-formatted list of {num_prompts} complex prompts (questions or instruct tasks) that cover a wide range of skills and knowledge areas related to the themes of {themes}. "
            # f"Generate a python-formatted list of {num_prompts} complex prompts related to the theme '{themes}'. "
            f"Each of these prompts should: "
            f"\n- have a complexity level of {complexity_level} out of 20 and a relevance level to the theme of {relevance_level} out of 20"
            f"\n- test various cognitive abilities ({abilities}) and require different types of writting formats ({formats})"
            f"\n- challenge the model's ability to understand and respond appropriately"
            f"\n- varyingly explore the {themes} in a manner that is consistent with their assigned complexity and relevance levels to the theme"
            # f"\nOnly output the prompts formatted as comma-separated, quote-encapsulated strings: [\"prompt1\", \"prompt2\", \"prompt3\"]"
            f"\nOutput each prompt on a new line without any extra commentary or special characters."
        )
        print(f"\n\n{prompt}\n")
        return prompt

    def generate_role(self) -> str:
        role = "You are a prompt engineer tasked with prompts of varying complexity to test the capabilities of a new language model. For each prompt, consider what aspect of the language model's capabilities it is designed to test and ensure that the set of prompts covers a broad spectrum of potential use cases for the language model. Only output the prompts, one per line without any extra commentary. Do not use any special characters or formatting, numbering or styling in the output."
        return role

    def tokenize(self, prompt: str, role: str) -> str:
        role_templates = {
            "system": "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n{{{{ {} }}}}<|eot_id|>",
            "user": "<|start_header_id|>user<|end_header_id|>\n{{{{ {} }}}}<|eot_id|>",
            "assistant": "<|start_header_id|>assistant<|end_header_id|>\n{{{{ {} }}}}<|eot_id|>",
            "end": "<|start_header_id|>assistant<|end_header_id|>",
        }
        msgs = [
            {"role": "system", "content": role},
            {"role": "user", "content": prompt},
        ]
        full_prompt = io.StringIO()
        for msg in msgs:
            full_prompt.write(role_templates[msg["role"]].format(msg["content"]))
        full_prompt.write(role_templates["end"])
        return full_prompt.getvalue()

    def generate(self):
        prompt = self.generate_prompt()
        role = self.generate_role()
        return self.tokenize(prompt, role)


def clean_line(line: str) -> str:
    line = line.strip()
    # remove list numbering
    line = re.sub(r"^\s*\d+\.?\s*", "", line)
    return line


def parse_output(output: str) -> list[str]:
    lines = output.split("\n")
    lines = [clean_line(line) for line in lines]
    # filter out null lines or prompts that are too short or long
    lines = [line for line in lines if (len(line) > 10 and len(line) < 300)]
    # skip first line as that's frequently broken
    return lines[1:]


def check_prompts_quality(prompts: list[str]):
    counter = collections.Counter(prompts)
    freqs = []
    for prompt, frequency in counter.items():
        if frequency > 1:
            freqs.append(frequency)
    # count the frequency of the frequencies
    freq_counter = collections.Counter(freqs)
    for freq, num in freq_counter.items():
        print(f"Found {num} prompts with {freq} duplicates")

    if not freq_counter:
        print("All prompts generated are unique")


def save_to_file(prompts: list[str], filepath: str = "prompts.txt"):
    try:
        with open(filepath, "a") as f:
            for prompt in prompts:
                f.write(prompt + "\n")
        print(f"Saved prompts to: {filepath}")
    except IOError as e:
        print(f"Error while saving prompts: {e}")


def generate_prompts(
    model,
    tokenizer,
    total_prompts,
    batch_size: int = 5,
    num_return_sequences: int = 5,
    max_new_tokens: int = 2000,
    temperature: float = 1.0,
):
    prompt_generator = PromptGeneratingPrompt()
    num_prompts = 0

    i = 0
    while num_prompts < total_prompts:
        prompts = [prompt_generator.generate() for _ in range(batch_size)]

        tokenizer.pad_token = tokenizer.eos_token
        inputs = tokenizer(prompts, return_tensors="pt", padding=True).to("cuda")

        start_ts = datetime.datetime.now()
        responses = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            num_return_sequences=num_return_sequences,
            do_sample=True,  # use sampling-based decoding
        )
        seconds_taken = (datetime.datetime.now() - start_ts).total_seconds()
        print(f"{i=}: generation took {seconds_taken:.2f}s\n")

        new_prompts = []
        for j, response in enumerate(responses):
            output = tokenizer.decode(response, skip_special_tokens=True)
            if output == "":
                print(f"{i=}: model generation returned empty output")
                continue

            # remove the input prompts from the output
            prompt_ending = prompt_generator.prompt_ending
            idx = output.find(prompt_ending) + len(prompt_ending)
            output = output[idx:].strip()
            # print(f"{i=}: output: {output}\n")

            generated_prompts = parse_output(output)
            # print(f"{i=}: prompts: {generated_prompts}\n")

            if generated_prompts is None:
                print(f"{i=}: could not parse prompts from: {output}\n")
                continue

            print(f"{i=}: {j=} generated {len(generated_prompts)} prompts\n")
            new_prompts.extend(generated_prompts)

        # check_prompts_quality(new_prompts)

        # remove any duplicates
        new_prompts = list(set(new_prompts))
        save_to_file(new_prompts, f"prompts_{i}.txt")
        num_prompts += len(new_prompts)

        i += 1

    print(f"\nTotal unique prompts generated: {num_prompts}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate prompts")
    parser.add_argument(
        "--total_prompts",
        type=int,
        default=20,
        help="Total number of prompts to generate",
    )
    parser.add_argument(
        "--quantize",
        action="store_true",
        help="Quantize the model",
        default=False,
    )
    parser.add_argument(
        "--model_name",
        type=str,
        default=None,  # "meta-llama/Meta-Llama-3.1-8B-Instruct",
        help="Model name to use",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=5,
        help="Batch size",
    )
    parser.add_argument(
        "--num_return_sequences",
        type=int,
        default=5,
        help="Number of return sequences",
    )
    parser.add_argument(
        "--max_new_tokens",
        type=int,
        default=2000,
        help="Max new tokens",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=1.0,
        help="Temperature",
    )
    parser.add_argument(
        "--model_path",
        type=str,
        default="./llama/",
        help="Path to save the model",
    )

    args = parser.parse_args()

    quantization_config = None
    if args.quantize:
        from transformers import BitsAndBytesConfig

        quantization_config = BitsAndBytesConfig(
            llm_int8_enable_fp32_cpu_offload=False,
            load_in_4bit=True,
            bnb_4bit_compute_dtype=torch.float16,
        )
        print("Model Quantized")

    model = AutoModelForCausalLM.from_pretrained(
        args.model_name if args.model_name else args.model_path,
        device_map="auto",
        # use_auth_token=True,
        quantization_config=quantization_config,
        local_files_only=True,
    )
    # model.save_pretrained("./llama")

    tokenizer = AutoTokenizer.from_pretrained(
        # "meta-llama/Meta-Llama-3.1-8B-Instruct")
        args.model_name if args.model_name else args.model_path,
        local_files_only=True,
    )
    # tokenizer.save_pretrained("./llama")

    start_ts = datetime.datetime.now()
    generate_prompts(
        model,
        tokenizer,
        total_prompts=args.total_prompts,
        batch_size=args.batch_size,
        num_return_sequences=args.num_return_sequences,
        max_new_tokens=args.max_new_tokens,
        temperature=args.temperature,
    )
    seconds_taken = (datetime.datetime.now() - start_ts).total_seconds()

    print(f"Full Generation took {seconds_taken:.2f} seconds")
