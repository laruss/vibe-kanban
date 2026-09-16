import { create, useModal } from '@ebay/nice-modal-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@vibe/ui/components/KeyboardDialog';
import { Button } from '@vibe/ui/components/Button';
import { WarningIcon } from '@phosphor-icons/react';
import { defineModal } from '@/shared/lib/modals';

export type RunningStageMoveResult = 'cancel' | 'continue' | 'stop';

interface RunningStageMoveDialogProps {
  issueCount: number;
}

const RunningStageMoveDialogImpl = create<RunningStageMoveDialogProps>(
  ({ issueCount }) => {
    const modal = useModal();
    const resolve = (result: RunningStageMoveResult) => {
      modal.resolve(result);
      modal.hide();
    };

    return (
      <Dialog
        open={modal.visible}
        onOpenChange={(open) => !open && resolve('cancel')}
      >
        <DialogContent className="sm:max-w-[520px]">
          <DialogHeader>
            <div className="flex items-center gap-base">
              <WarningIcon className="size-icon-lg text-brand" weight="bold" />
              <DialogTitle>Agent stage is still running</DialogTitle>
            </div>
            <DialogDescription className="pt-base text-left">
              {issueCount === 1
                ? 'Moving this task leaves its current agent stage behind.'
                : `Moving these tasks leaves ${issueCount} running agent stages behind.`}{' '}
              You can keep the agent running and retain its result in history,
              but it will no longer advance the manually moved task.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter className="flex-wrap gap-half sm:justify-end">
            <Button variant="outline" onClick={() => resolve('cancel')}>
              Cancel
            </Button>
            <Button variant="outline" onClick={() => resolve('continue')}>
              Move and keep running
            </Button>
            <Button variant="destructive" onClick={() => resolve('stop')}>
              Stop agent and move
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    );
  }
);

export const RunningStageMoveDialog = defineModal<
  RunningStageMoveDialogProps,
  RunningStageMoveResult
>(RunningStageMoveDialogImpl);
