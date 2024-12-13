package blueskybot

import (
	"bytes"
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	cli "github.com/urfave/cli/v2"
)

var LabelCmd = &cli.Command{
	Name: "label",
	Flags: cmd.CombineFlags(
		cmd.WithDb,
		&cli.BoolFlag{
			Name:  "neg",
			Usage: "negate a label value",
			Value: false,
		},
		&cli.StringFlag{
			Name:     "label",
			Usage:    "name of the label to apply or negate",
			Required: true,
		},
	),
	Action: func(cctx *cli.Context) error {
		name := cctx.String("label")
		if name == "" {
			return fmt.Errorf("label argument is required")
		}

		d := dbx.NewDBx(cmd.ToContext(cctx))
		bday, err := d.Labels.FindOrCreateLabel(name)
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
				Val: name,
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
