import os.path
import torch
import pandas as pd
from torch.nn.utils.rnn import pad_sequence
from torch.nn.functional import pad
from typing import List


class SampleDataset(torch.utils.data.Dataset):
    def __init__(
        self,
        labels: pd.DataFrame,
        original_mimic_path: str,
        processed_mimic_path: str,
        stay_ids: List[int] = None,
        feature_ids: List[int] = None,
    ):

        if stay_ids is None:
            self.stay_ids = [
                int(dirname) for dirname in os.listdir(processed_mimic_path)
            ]
        else:
            self.stay_ids = stay_ids

        if feature_ids is None:
            self.itemids = pd.read_csv(f"{original_mimic_path}/icu/d_items.csv")[
                "itemid"
            ].to_list()
        else:
            self.itemids = feature_ids

        self.labels = labels
        self.processed_mimic_path = processed_mimic_path

    @staticmethod
    def truncate_collate(batch):
        x_padded = pad_sequence([X for X, _ in batch], batch_first=True)
        y = torch.stack([Y for _, Y in batch], dim=0)
        return x_padded.float()[:, 0, :], y.float()  # TODO: super simplistic for now

    def maxlen_padmask_collate(self, batch):
        # TODO: could find a better way to do this
        # Pad #1 first, then pad all others
        x_first, y_first = batch[0]
        x_first = pad(x_first, (0, 0, 0, self.max_len - x_first.shape[0]))
        batch[0] = (x_first, y_first)

        x_padded_0 = pad_sequence(
            [X for X, _ in batch], batch_first=True, padding_value=0.0
        )
        x_padded_42 = pad_sequence(
            [X for X, _ in batch], batch_first=True, padding_value=42.0
        )

        padding_mask = torch.logical_not(
            torch.logical_and(x_padded_0 == 0.0, x_padded_42 == 42.0)
        )[:, :, 0]

        y = torch.stack([Y for _, Y in batch], dim=0)
        return x_padded_0.float(), y.float(), padding_mask

    # Right-pad tensors in batch to the size of the largest
    # Old method (per-batch padding)
    @staticmethod
    def padding_collate(batch):
        x_padded = pad_sequence(
            [torch.transpose(X, 0, 1) for X, _ in batch], batch_first=True
        )
        # x_padded = torch.transpose(x_padded, 1, 2)
        y = torch.stack([Y for _, Y in batch], dim=0)
        return x_padded.float(), y.float()

    def __len__(self):
        return len(self.stay_ids)

    def __getitem__(self, index: int):
        stay_id = self.stay_ids[index]

        # Labels
        Y = self.labels.loc[stay_id]
        Y = torch.tensor(Y.values)

        # Features
        # Ensures every example has a sequence length of at least 1
        combined_features = pd.DataFrame(columns=["feature_id", "0"])

        for feature_file in [
            "chartevents_features.csv",
            "outputevents_features.csv",
            "inputevent_features.csv",
        ]:
            full_path = f"{self.processed_mimic_path}/{stay_id}/{feature_file}"

            if os.path.exists(full_path):
                combined_features = pd.concat(
                    [combined_features, pd.read_csv(full_path)]
                )

        # Make sure all itemids are represented in order, add 0-tensors where missing
        combined_features = combined_features.set_index("feature_id")
        combined_features = combined_features.reindex(
            self.itemids
        )  # Need to add any itemids that are missing
        # TODO: could probably do imputation better (but maybe during preprocessing)
        combined_features = combined_features.fillna(0.0)

        X = torch.tensor(combined_features.values)

        return X, Y


if __name__ == "__main__":
    import random

    stay_ids = [int(dirname) for dirname in os.listdir("./testcache")]
    labels = list()

    for sid in stay_ids:
        labels.append(random.random() > 0.5)

    labels = pd.DataFrame(index=stay_ids, data={"label": labels})

    ds = SampleDataset(labels, "./testmimic", "./testcache")

    dl = torch.utils.data.DataLoader(
        ds, collate_fn=ds.padding_collate, num_workers=2, batch_size=4, pin_memory=True
    )

    print("Printing first few batches:")
    for batchnum, (X, Y) in enumerate(dl):
        print(f"Batch number: {batchnum}")
        print(f"X shape: {X.shape}")
        print(f"Y shape: {Y.shape}")
        print(X)
        print(Y)

        if batchnum == 5:
            break
