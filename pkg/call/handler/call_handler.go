package call_handler

import (
	"errors"
	"net/http"

	call_service "github.com/EvolutionAPI/evolution-go/pkg/call/service"
	instance_model "github.com/EvolutionAPI/evolution-go/pkg/instance/model"
	"github.com/gin-gonic/gin"
)

type CallHandler interface {
	Dial(ctx *gin.Context)
	Answer(ctx *gin.Context)
	RejectCall(ctx *gin.Context)
	Hangup(ctx *gin.Context)
	Current(ctx *gin.Context)
	Offer(ctx *gin.Context)
	Candidate(ctx *gin.Context)
	Heartbeat(ctx *gin.Context)
	ICEConfig(ctx *gin.Context)
	DeploymentDrain(ctx *gin.Context)
}

type callHandler struct {
	callService call_service.CallService
}

// Reject call
// @Summary Reject call
// @Description Reject call
// @Tags Call
// @Accept json
// @Produce json
// @Param message body call_service.RejectCallStruct true "Call data"
// @Success 200 {object} gin.H "success"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /call/reject [post]
func (g *callHandler) RejectCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *call_service.RejectCallStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := g.callService.RejectCall(data, instance)
	if err != nil {
		callError(ctx, err)
		return
	}
	if data.ControllerID != "" || data.CallCreator.IsEmpty() {
		ctx.JSON(http.StatusOK, result)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

func (g *callHandler) Dial(ctx *gin.Context) {
	var data call_service.DialCallStruct
	if err := ctx.ShouldBindJSON(&data); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	instance, ok := callInstance(ctx)
	if !ok {
		return
	}
	result, err := g.callService.Dial(&data, instance)
	if err != nil {
		callError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, result)
}

func (g *callHandler) Answer(ctx *gin.Context) {
	var data call_service.AnswerCallStruct
	if err := ctx.ShouldBindJSON(&data); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	instance, ok := callInstance(ctx)
	if !ok {
		return
	}
	result, err := g.callService.Answer(&data, instance)
	if err != nil {
		callError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, result)
}

func (g *callHandler) Hangup(ctx *gin.Context) {
	var data call_service.ControlCallStruct
	if err := ctx.ShouldBindJSON(&data); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	instance, ok := callInstance(ctx)
	if !ok {
		return
	}
	result, err := g.callService.Hangup(&data, instance)
	if err != nil {
		callError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, result)
}

func (g *callHandler) Current(ctx *gin.Context) {
	instance, ok := callInstance(ctx)
	if !ok {
		return
	}
	if result, found := g.callService.Current(instance); found {
		response := gin.H{"call": result}
		if sampler, ok := g.callService.(interface {
			CurrentMediaDiagnostics(*instance_model.Instance, string) (call_service.CallMediaDiagnostics, bool)
		}); ok {
			if diagnostics, available := sampler.CurrentMediaDiagnostics(instance, result.CallID); available {
				response["mediaDiagnostics"] = diagnostics
			}
		}
		ctx.JSON(http.StatusOK, response)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"call": nil})
}

func (g *callHandler) Offer(ctx *gin.Context) {
	var data call_service.OfferCallStruct
	if err := ctx.ShouldBindJSON(&data); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	instance, ok := callInstance(ctx)
	if !ok {
		return
	}
	sdp, err := g.callService.Offer(&data, instance)
	if err != nil {
		callError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"callId": data.CallID, "sdp": sdp, "type": "answer"})
}

func (g *callHandler) Candidate(ctx *gin.Context) {
	var data call_service.CandidateCallStruct
	if err := ctx.ShouldBindJSON(&data); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	instance, ok := callInstance(ctx)
	if !ok {
		return
	}
	if err := g.callService.Candidate(&data, instance); err != nil {
		callError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

func (g *callHandler) Heartbeat(ctx *gin.Context) {
	var data call_service.HeartbeatCallStruct
	if err := ctx.ShouldBindJSON(&data); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	instance, ok := callInstance(ctx)
	if !ok {
		return
	}
	if err := g.callService.Heartbeat(&data, instance); err != nil {
		callError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

func (g *callHandler) ICEConfig(ctx *gin.Context) {
	instance, ok := callInstance(ctx)
	if !ok {
		return
	}
	result, err := g.callService.ICEConfig(instance)
	if err != nil {
		callError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, result)
}

func (g *callHandler) DeploymentDrain(ctx *gin.Context) {
	var data call_service.DrainRequest
	if err := ctx.ShouldBindJSON(&data); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result, err := g.callService.DeploymentDrain(&data)
	if err != nil {
		callError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, result)
}

func callInstance(ctx *gin.Context) (*instance_model.Instance, bool) {
	instance, ok := ctx.MustGet("instance").(*instance_model.Instance)
	if !ok || instance == nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return nil, false
	}
	return instance, true
}

func callError(ctx *gin.Context, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, call_service.ErrDisabled):
		status = http.StatusServiceUnavailable
	case errors.Is(err, call_service.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, call_service.ErrOwner), errors.Is(err, call_service.ErrCallConflict):
		status = http.StatusForbidden
	case errors.Is(err, call_service.ErrCallPeerNotAllowed):
		status = http.StatusForbidden
	case errors.Is(err, call_service.ErrCallBusy), errors.Is(err, call_service.ErrCallCapacity):
		status = http.StatusConflict
	case errors.Is(err, call_service.ErrCallDrainConflict):
		status = http.StatusConflict
	case errors.Is(err, call_service.ErrCallEnded):
		status = http.StatusConflict
	}
	ctx.JSON(status, gin.H{"error": err.Error()})
}

func NewCallHandler(
	callService call_service.CallService,
) CallHandler {
	return &callHandler{
		callService: callService,
	}
}
