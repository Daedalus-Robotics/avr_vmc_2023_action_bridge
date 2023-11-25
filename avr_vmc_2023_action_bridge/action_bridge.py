import json
import time
from typing import List, Tuple, Any

import rclpy
from rclpy import Future
from rclpy.action import ActionClient
from rclpy.action.client import ClientGoalHandle
from rclpy.node import Node

from avr_vmc_2023_action_bridge_interfaces.srv import Goal, Cancel
from avr_vmc_2023_action_bridge_interfaces.msg import Feedback, Result
from avr_vmc_2023_auton_drop_interfaces.action import AutonDrop


class ActionBridgeNode(Node):
    def __init__(self) -> None:
        super().__init__('avr_vmc_2023_action_bridge', namespace='action_bridge')

        self.goal_service = self.create_service(
            Goal,
            'goal',
            self.send_goal
        )
        self.cancel_service = self.create_service(
            Cancel,
            'cancel',
            self.cancel
        )

        self.feedback_publisher = self.create_publisher(
            Feedback,
            'feedback',
            10
        )
        self.result_publisher = self.create_publisher(
            Result,
            'result',
            10
        )

        self.action_clients: List[Tuple[str, type, ActionClient]] = []
        # Setup actions
        self.action_clients.append(
            (
                '/auton_drop/trigger',
                AutonDrop,
                ActionClient(
                    self,
                    AutonDrop,
                    '/auton_drop/trigger'
                )
            )
        )

        for client_info in self.action_clients:
            # noinspection PyProtectedMember
            self.get_logger().info(f'Waiting for action: {client_info[0]}')
            success = client_info[2].wait_for_server(timeout_sec=10)
            if not success:
                # noinspection PyProtectedMember
                self.get_logger().warning(f'Could not find action: {client_info[0]}')

        self.goal_handles: List[ClientGoalHandle | None] = [None] * len(self.action_clients)

        self.get_logger().info('Started')

    async def send_goal(self, request: Goal.Request, response: Goal.Response) -> Goal.Response:
        if 0 <= request.id < len(self.action_clients):
            client_info = self.action_clients[request.id]
            try:
                data = json.loads(request.data)
            except json.JSONDecodeError:
                self.get_logger().warning(f'Failed to parse goal message for action: {client_info[1]}')
                self.get_logger().warning(f'Data: {request.data}')
            else:
                self.get_logger().debug(f'Calling action \'{client_info[0]}\' with data: {data}')
                # noinspection PyUnresolvedReferences
                goal = client_info[1].Goal(**data)
                handle_future: Future = client_info[2].send_goal_async(
                    goal,
                    feedback_callback=lambda msg: self._send_feedback(request.id, msg)
                )
                handle_future.add_done_callback(lambda fut: self._goal_handle_callback(request.id, fut))

                response.success = True
        return response

    def _goal_handle_callback(self, goal_id: int, handle_future: Future) -> None:
        handle = handle_future.result()

        result_future: Future = handle.get_result_async()
        result_future.add_done_callback(lambda future: self._send_result(goal_id, future))

        self.goal_handles[goal_id] = handle

    def cancel(self, request: Cancel.Request, response: Cancel.Response) -> Cancel.Response:
        if 0 <= request.id < len(self.action_clients):
            client_name = self.action_clients[request.id][0]
            if self.goal_handles[request.id] is not None:
                self.get_logger().debug(f'Canceling action \'{client_name}\'')
                self.goal_handles[request.id].cancel_goal_async()
                response.success = True
        return response

    def _send_feedback(self, action_id: int, msg: Any) -> None:
        client_name = self.action_clients[action_id][0]
        self.get_logger().debug(f'Sending feedback for action: {client_name}')

        feedback = Feedback()
        feedback.id = action_id
        feedback.data = self._convert_msg_to_json(msg)

        self.feedback_publisher.publish(feedback)

    def _send_result(self, action_id: int, future: Future) -> None:
        client_name = self.action_clients[action_id][0]
        self.get_logger().debug(f'Action \'{client_name}\' has finished')

        result: Any = future.result().result
        result_msg = Result()
        result_msg.id = action_id
        result_msg.data = self._convert_msg_to_json(result)

        self.result_publisher.publish(result_msg)

    @staticmethod
    def _convert_msg_to_json(msg: Any) -> str:
        d = {}
        for slot in msg.__slots__:
            value = getattr(msg, slot)
            d[slot] = value
        return json.dumps(d)


def main() -> None:
    rclpy.init()
    node = ActionBridgeNode()
    executor = rclpy.executors.MultiThreadedExecutor()
    rclpy.spin(node, executor)


if __name__ == '__main__':
    main()
