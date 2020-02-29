import argparse
from time import time, sleep
import os
from glob import glob
import boutiques2pydra as b2p 

import pydra
import typing as ty

import nibabel as nib
import numpy as np

def group_analysis(wf_results):
    brain_sizes = []
    for res in wf_results:
        data = nib.load(res.output.out[0].decode('utf-8')).get_fdata()
        brain_sizes.append((data != 0).sum())

    return np.array(brain_sizes).mean()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Example BIDS app using Pydra and Boutiques (performs participant and group analysis)"
    )
    parser.add_argument(
        "bids_dir",
        help="The directory with the input dataset "
        "formatted according to the BIDS standard.",
    )
    parser.add_argument(
        "output_dir",
        help="The directory where the output files "
        "should be stored. If you are running group level analysis "
        "this folder should be prepopulated with the results of the"
        "participant level analysis.",
    )
    args = parser.parse_args()

    subject_dirs = glob(os.path.join(args.bids_dir, "sub-*"))
    subjects_to_analyze = [subject_dir.split("-")[-1] for subject_dir in subject_dirs]

    wf = pydra.Workflow(
        name="BIDS App Example with Boutiques",
        input_spec=["infile", "maskfile"]
    )

    T1_files = [
        os.path.abspath(T1_file)
        for subject_label in subjects_to_analyze
        for T1_file in glob(
            os.path.join(args.bids_dir, "sub-%s" % subject_label, "anat", "*_T1w.nii*")
        )
        + glob(
            os.path.join(
                args.bids_dir, "sub-%s" % subject_label, "ses-*", "anat", "*_T1w.nii*"
            )
        )
    ]

    mask_files = [os.path.abspath(os.path.join(
                    args.output_dir,
                        (
                            os.path.split(t1f)[-1]
                            .replace("_T1w", "_brain")
                            .replace(".gz", "")
                            .replace(".nii", "")
                        ),
                    ))
                    for t1f in T1_files
                 ]

    wf.split(("infile", "maskfile"), infile=T1_files, maskfile=mask_files)

    wf.add(
        b2p.Boutiques2Pydra("zenodo.3267250",
                            "-v{0}:{0}".format(os.path.abspath(args.bids_dir)),
                            "-v{0}:{0}".format(os.path.abspath(args.output_dir)),
                            input_spec=["infile", "maskfile"]
                                ).create_task(
                                name="fsl_bet",
                                infile=wf.lzin.infile,
                                maskfile=wf.lzin.maskfile
                            )
        )

    #wf.add(group_analysis(name="group_analysis", brain_files=wf.fsl_bet.lzout.bet_out))

    wf.set_output([("out", wf.fsl_bet.lzout.outfile)])

    with pydra.Submitter(plugin="cf") as sub:
        sub(wf)
    print(wf.result())
    print("Group analysis result:", group_analysis(wf.result()))
