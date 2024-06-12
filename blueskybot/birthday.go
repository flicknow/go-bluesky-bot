package blueskybot

import (
	"bytes"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	cli "github.com/urfave/cli/v2"
)

var BirthdayCmd = &cli.Command{
	Name: "birthday",
	Flags: cmd.CombineFlags(
		cmd.WithDb,
		&cli.BoolFlag{
			Name:  "neg",
			Usage: "negate a label value",
			Value: false,
		},
	),
	Action: func(cctx *cli.Context) error {
		d := dbx.NewDBx(cmd.ToContext(cctx))
		bday, err := d.Labels.FindOrCreateLabel("birthday")
		if err != nil {
			return err
		}

		now := time.Now()
		labels := make([]*dbx.CustomLabel, cctx.Args().Len())
		for i, user := range cctx.Args().Slice() {
			did, err := lookupDid(user)
			if err != nil {
				return err
			}

			actor, err := d.Actors.FindOrCreateActor(did)
			if err != nil {
				return err
			}

			neg := cctx.Bool("neg")
			ver := int64(1)
			label := &atproto.LabelDefs_Label{
				Cts: now.UTC().Format(time.RFC3339),
				Src: LabelerDid,
				Uri: actor.Did,
				Val: "birthday",
				Ver: &ver,
			}
			if neg {
				label.Neg = &neg
			}

			sigBuf := new(bytes.Buffer)
			err = label.MarshalCBOR(sigBuf)
			if err != nil {
				return err
			}

			sigBytes, err := d.SigningKey.HashAndSign(sigBuf.Bytes())
			if err != nil {
				return err
			}
			label.Sig = sigBytes

			cborBuf := new(bytes.Buffer)
			err = label.MarshalCBOR(cborBuf)
			if err != nil {
				return err
			}

			labels[i] = &dbx.CustomLabel{
				SubjectType: dbx.AccountLabelType,
				SubjectId:   actor.ActorId,
				CreatedAt:   now.Unix(),
				LabelId:     bday.LabelId,
				Neg:         0,
				Cbor:        cborBuf.Bytes(),
			}
			if neg {
				labels[i].Neg = 1
			}
		}
		return d.CustomLabels.InsertLabels(labels)
	},
}
